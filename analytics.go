package analytics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Version of the client.
const Version = "4.0.0"

// Client is the main API exposed by the analytics package.
// Values that satsify this interface are returned by the client constructors
// provided by the package and provide a way to send messages via the HTTP API.
type Client interface {
	io.Closer

	// Queues a message to be sent by the client when the conditions for a batch
	// upload are met.
	// This is the main method you'll be using, a typical flow would look like
	// this:
	//
	//	client := analytics.New(writeKey)
	//	...
	//	client.Enqueue(analytics.Track{ ... })
	//	...
	//	client.Close()
	//
	// The method returns an error if the message queue not be queued, which
	// happens if the client was already closed at the time the method was
	// called or if the message was malformed.
	Enqueue(Message) error
}

type client struct {
	Config
	key string

	// This channel is where the `Enqueue` method writes messages so they can be
	// picked up and pushed by the backend goroutine taking care of applying the
	// batching rules.
	msgs chan Message

	// These two channels are used to synchronize the client shutting down when
	// `Close` is called.
	// The first channel is closed to signal the backend goroutine that it has
	// to stop, then the second one is closed by the backend goroutine to signal
	// that it has finished flushing all queued messages.
	quit     chan struct{}
	shutdown chan struct{}

	// This HTTP client is used to send requests to the backend, it uses the
	// HTTP transport provided in the configuration.
	http http.Client
}

// NewDiscardClient returns client which discards all messages.
func NewDiscardClient() Client {
	return discardClient{}
}

type discardClient struct{}

func (c discardClient) Close() error          { return nil }
func (c discardClient) Enqueue(Message) error { return nil }

// New instantiates a new client that uses the write key passed as first
// argument to send messages to the backend. The client is created with the
// default (empty) configuration.
func New(writeKey string) Client {
	c, err := NewWithConfig(writeKey, Config{})
	if err != nil {
		// Default config should be always valid hence if error there's a bug in API
		// and better to stop.
		panic(err)
	}
	return c
}

// NewWithConfig instantiates a new client that uses the write key and
// configuration passed as arguments to send messages to the backend. The
// function will return an error if the configuration contained impossible
// values (like a negative flush interval for example). When the function
// returns an error the returned client will always be nil.
func NewWithConfig(writeKey string, config Config) (Client, error) {
	c, err := newWithConfig(writeKey, config)
	if err != nil {
		return nil, err
	}
	go c.loop()

	return c, nil
}

func newWithConfig(writeKey string, config Config) (*client, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	c := &client{
		Config:   makeConfig(config),
		key:      writeKey,
		msgs:     make(chan Message, 100),
		quit:     make(chan struct{}),
		shutdown: make(chan struct{}),
		http:     makeHTTPClient(config.Transport),
	}

	return c, nil
}

func makeHTTPClient(transport http.RoundTripper) http.Client {
	httpClient := http.Client{
		Transport: transport,
	}
	if supportsTimeout(transport) {
		httpClient.Timeout = 10 * time.Second
	}
	return httpClient
}

func (c *client) Enqueue(msg Message) (err error) {
	if err = msg.validate(); err != nil {
		c.notifyDropped(msg, err, 1)
		return
	}

	var id = c.uid()
	var ts = c.now()

	switch m := msg.(type) {
	case Alias:
		m.Type = "alias"
		m.MessageId = makeMessageID(m.MessageId, id)
		m.Timestamp = makeTimestamp(m.Timestamp, ts)
		msg = m

	case Group:
		m.Type = "group"
		m.MessageId = makeMessageID(m.MessageId, id)
		m.Timestamp = makeTimestamp(m.Timestamp, ts)
		msg = m

	case Identify:
		m.Type = "identify"
		m.MessageId = makeMessageID(m.MessageId, id)
		m.Timestamp = makeTimestamp(m.Timestamp, ts)
		msg = m

	case Page:
		m.Type = "page"
		m.MessageId = makeMessageID(m.MessageId, id)
		m.Timestamp = makeTimestamp(m.Timestamp, ts)
		msg = m

	case Screen:
		m.Type = "screen"
		m.MessageId = makeMessageID(m.MessageId, id)
		m.Timestamp = makeTimestamp(m.Timestamp, ts)
		msg = m

	case Track:
		m.Type = "track"
		m.MessageId = makeMessageID(m.MessageId, id)
		m.Timestamp = makeTimestamp(m.Timestamp, ts)
		msg = m
	case TrackObj:
		m.Type = "track"
		m.MessageId = makeMessageID(m.MessageId, id)
		m.Timestamp = makeTimestamp(m.Timestamp, ts)
		msg = m
	case TrackObjLess:
		m.Type = "track"
		msg = m
	}

	defer func() {
		// When the `msgs` channel is closed writing to it will trigger a panic.
		// To avoid letting the panic propagate to the caller we recover from it
		// and instead report that the client has been closed and shouldn't be
		// used anymore.
		if recover() != nil {
			err = ErrClosed
			c.notifyDropped(msg, err, 1)
		}
	}()

	c.msgs <- msg
	return
}

// Close and flush metrics.
func (c *client) Close() (err error) {
	defer func() {
		// Always recover, a panic could be raised if `c`.quit was closed which
		// means the method was called more than once.
		if recover() != nil {
			err = ErrClosed
		}
	}()
	close(c.quit)
	<-c.shutdown
	return
}

// Asychronously send a batched requests.
func (c *client) sendAsync(msgs []message, wg *sync.WaitGroup, ex *executor) {
	wg.Add(1)

	if !ex.do(func() {
		defer wg.Done()
		defer func() {
			// In case a bug is introduced in the send function that triggers
			// a panic, we don't want this to ever crash the application so we
			// catch it here and log it instead.
			if err := recover(); err != nil {
				c.errorf("panic - %s", err)
			}
		}()
		c.send(msgs)
	}) {
		wg.Done()
		c.errorf("sending messages failed - %s", ErrTooManyRequests)
		c.notifyFailure(msgs, ErrTooManyRequests)
	}
}

// Send batch request.
func (c *client) send(msgs []message) {
	const attempts = 10

	b, err := json.Marshal(batch{
		MessageID: c.uid(),
		SentAt:    c.now(),
		Messages:  msgs,
		Context:   c.DefaultContext,
	})

	if err != nil {
		c.errorf("marshalling messages - %s", err)
		c.notifyFailure(msgs, err)
		return
	}

	for i := 0; i != attempts; i++ {
		if err = c.upload(b); err == nil {
			c.notifySuccess(msgs)
			return
		}

		// Wait for either a retry timeout or the client to be closed.
		select {
		case <-time.After(c.RetryAfter(i)):
		case <-c.quit:
			err = fmt.Errorf("%d messages dropped because they failed to be sent and the client was closed", len(msgs))
			c.errorf(err.Error())
			c.notifyFailure(msgs, err)
			return
		}
	}

	c.errorf("%d messages dropped because they failed to be sent after %d attempts", len(msgs), attempts)
	c.notifyFailure(msgs, err)
}

// Upload serialized batch message.
func (c *client) upload(b []byte) error {
	url := c.Endpoint
	req, err := http.NewRequest("POST", url, bytes.NewReader(b))
	if err != nil {
		c.errorf("creating request - %s", err)
		return err
	}

	req.Header.Add("User-Agent", "analytics-go (version: "+Version+")")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Content-Length", strconv.Itoa(len(b)))
	req.Header.Add("x-api-key", c.key)

	res, err := c.http.Do(req)

	if err != nil {
		return err
	}

	defer res.Body.Close()
	return c.report(res)
}

// Report on response body.
func (c *client) report(res *http.Response) (err error) {
	var body []byte

	if res.StatusCode < 300 {
		c.debugf("response %s", res.Status)
		return
	}

	if body, err = io.ReadAll(res.Body); err != nil {
		c.errorf("response %d %s - %s", res.StatusCode, res.Status, err)
		return
	}

	c.logf("response %d %s – %s", res.StatusCode, res.Status, string(body))
	return fmt.Errorf("%d %s", res.StatusCode, res.Status)
}

// Batch loop.
func (c *client) loop() {
	defer close(c.shutdown)

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	tick := time.NewTicker(c.Interval)
	defer tick.Stop()

	ex := newExecutor(c.maxConcurrentRequests)
	defer ex.close()

	mq := messageQueue{
		maxBatchSize:  c.BatchSize,
		maxBatchBytes: c.maxBatchBytes(),
	}

	for {
		select {
		case msg := <-c.msgs:
			c.push(&mq, msg, wg, ex)

		case <-tick.C:
			c.flush(&mq, wg, ex)

		case <-c.quit:
			c.debugf("exit requested – draining messages")

			// Drain the msg channel, we have to close it first so no more
			// messages can be pushed and otherwise the loop would never end.
			close(c.msgs)
			for msg := range c.msgs {
				c.push(&mq, msg, wg, ex)
			}

			c.flush(&mq, wg, ex)
			c.debugf("exit")
			return
		}
	}
}

func (c *client) push(q *messageQueue, m Message, wg *sync.WaitGroup, ex *executor) {
	var msg message
	var err error

	if msg, err = makeMessage(m, maxMessageBytes); err != nil {
		c.errorf("%s - %v", err, m)
		c.notifyFailure([]message{msg}, err)
		return
	}

	c.debugf("buffer (%d/%d) %v", len(q.pending), c.BatchSize, m)

	if msgs := q.push(msg); msgs != nil {
		c.debugf("exceeded messages batch limit with batch of %d messages – flushing", len(msgs))
		c.sendAsync(msgs, wg, ex)
	}
}

func (c *client) flush(q *messageQueue, wg *sync.WaitGroup, ex *executor) {
	if msgs := q.flush(); msgs != nil {
		c.debugf("flushing %d messages", len(msgs))
		c.sendAsync(msgs, wg, ex)
	}
}

func (c *client) debugf(format string, args ...interface{}) {
	if c.Verbose {
		c.logf(format, args...)
	}
}

func (c *client) logf(format string, args ...interface{}) {
	c.Logger.Logf(format, args...)
}

func (c *client) errorf(format string, args ...interface{}) {
	c.Logger.Errorf(format, args...)
}

func (c *client) maxBatchBytes() int {
	b, _ := json.Marshal(batch{
		MessageID: c.uid(),
		SentAt:    c.now(),
		Context:   c.DefaultContext,
	})
	return maxBatchBytes - len(b)
}

func (c *client) reportMetrics(name string, value int64, tags []string) {
	if c.DDStatsdClient == nil {
		return
	}

	err := c.Config.DDStatsdClient.Count("submitted.success", value, tags, 1)
	if err != nil {
		c.errorf("error submitting metric %s - %s", name, err)
	}
}

func (c *client) notifySuccess(msgs []message) {
	for _, m := range msgs {
		c.reportMetrics("submitted.success", 1, m.Msg().tags())
	}
	if c.Callback != nil {
		for _, m := range msgs {
			c.Callback.Success(m.Msg())
		}
	}
}

func (c *client) notifyFailure(msgs []message, err error) {
	for _, m := range msgs {
		c.reportMetrics("submitted.failure", 1, m.Msg().tags())
	}
	if c.Callback != nil {
		for _, m := range msgs {
			c.Callback.Failure(m.Msg(), err)
		}
	}
}

func (c *client) notifyDropped(m Message, err error, count int64) {
	c.reportMetrics("dropped", count, m.tags())
}

func (c *client) notifyFailureMsg(m Message, err error, count int64) {
	c.reportMetrics("submitted.failure", count, m.tags())
	if c.Callback != nil {
		c.Callback.Failure(m, err)
	}
}

func (c *client) notifySuccessMsg(m Message, count int64) {
	c.reportMetrics("submitted.success", count, m.tags())
	if c.Callback != nil {
		c.Callback.Success(m)
	}
}
