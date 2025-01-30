package analytics

import (
	"fmt"

	"os"
	"strings"
	"sync"
	"time"

	ddstatsd "github.com/DataDog/datadog-go/v5/statsd"

	"github.com/rcrowley/go-metrics"
)

// Reporter provides a function to reporting metrics.
type Reporter interface {
	Report(metricName string, value interface{}, tags []string, ts time.Time)
	Flush() error
	AddTags(tags ...string)
}

func splitTags(name string) (string, []string) {
	tokens := strings.Split(name, ".")
	if len(tokens) <= 1 {
		return name, []string{}
	}
	names := []string{}
	tags := []string{}
	for _, token := range tokens {
		if strings.Contains(token, ":") {
			tags = append(tags, token)
		} else {
			names = append(names, token)
		}
	}
	return strings.Join(names, "."), tags
}

func (c *client) reportAll(prefix string, reporters []Reporter) {
	if len(reporters) == 0 {
		return
	}
	ts := time.Now()
	metrics := c.metricsRegistry.GetAll()
	go func() {
		for key, metric := range metrics {
			for measure, value := range metric {
				name, tags := splitTags(key)
				name = prefix + "." + name
				for _, r := range reporters {
					r.Report(name+"."+measure, value, tags, ts)
				}
			}
		}
		for _, r := range reporters {
			if err := r.Flush(); err != nil {
				c.Config.Logger.Errorf("flush failed for reporter %s: %s", r, err)
			}
		}
	}()
}

var hostname = func() string {
	h, err := os.Hostname()
	if err != nil {
		return "localhost"
	}
	return h
}()

// DiscardReporter discards all metrics, useful for tests.
type DiscardReporter struct{}

// Report reports metrics.
func (r DiscardReporter) Report(metricName string, value interface{}, tags []string, ts time.Time) {}

// AddTags adds tags to be added to each metric reported.
func (r *DiscardReporter) AddTags(tags ...string) {}

// Flush flushes reported metrics.
func (r *DiscardReporter) Flush() error { return nil }

// LogReporter report metrics as a log.
type LogReporter struct {
	logger Logger
	tags   []string
}

// NewLogReporter returns new log reporter ready to use.
func NewLogReporter(l ...Logger) *LogReporter {
	if len(l) == 0 {
		l = []Logger{newDefaultLogger()}
	}
	return &LogReporter{
		logger: l[0],
		tags:   []string{},
	}
}

// Report reports metrics.
func (r LogReporter) Report(metricName string, value interface{}, tags []string, ts time.Time) {
	allTags := append(tags, r.tags...)
	r.logger.Logf("%s[%s] = %v", metricName, strings.Join(allTags, ", "), value)
}

// Flush flushes reported metrics.
func (r *LogReporter) Flush() error { return nil }

// AddTags adds tags to be added to each metric reported.
func (r *LogReporter) AddTags(tags ...string) {
	r.tags = append(r.tags, tags...)
}

// NewDatadogReporter is a factory method to create Datadog reporter
// with sane defaults.
func NewDatadogReporter(client ddstatsd.ClientInterface, tags ...string) *DatadogReporter {
	dr := DatadogReporter{
		Client: client,
	}
	dr.logger = newDefaultLogger()
	dr.tags = append(tags, "transport:http", "sdkversion:go-"+Version)
	return &dr
}

// WithLogger sets logger to DatadogReporter.
func (dd *DatadogReporter) WithLogger(logger Logger) *DatadogReporter {
	dd.logger = logger
	return dd
}

// DatadogReporter reports metrics to DataDog.
type DatadogReporter struct {
	Client ddstatsd.ClientInterface
	logger Logger
	tags   []string
	sync.Mutex
}

// AddTags adds tags to be added to each metric reported.
func (dd *DatadogReporter) AddTags(tags ...string) {
	dd.Lock()
	defer dd.Unlock()
	dd.tags = append(dd.tags, tags...)
}

// Flush flushes reported metrics.
func (dd *DatadogReporter) Flush() error {
	err := dd.Client.Flush()
	if err != nil {
		dd.logger.Errorf("Flushing metrics failed: %s", err)
	}
	return err
}

// Report sends provided metric to Datadog.
func (dd *DatadogReporter) Report(metricName string, value interface{}, tags []string, ts time.Time) {
	metricValue, err := func() (float64, error) {
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case int:
			return float64(v), nil
		}
		return 0, fmt.Errorf("can't handle value %+v", value)
	}()
	if err != nil {
		dd.logger.Errorf("Serializing value for metric %s(%+v) failed: %s", metricName, value, err)
		return
	}
	allTags := append(tags, "hostname:"+hostname)
	allTags = append(allTags, dd.tags...)
	err = dd.Client.Gauge(metricName, metricValue, allTags, 1)
	if err != nil {
		dd.logger.Errorf("Reporting metric %s(%+v) failed: %s", metricName, value, err)
	}
}

func (c *client) resetMetrics() {
	ms := c.metricsRegistry.GetAll()
	for name := range ms {
		metric := c.metricsRegistry.Get(name)
		switch m := metric.(type) {
		case metrics.Counter:
			m.Clear()
		case metrics.Gauge:
			m.Update(0)
		case metrics.Histogram:
			// do nothing as Histogram has it's own internal cleanup
		}
	}
}

type countersFunc func(tags ...string) metrics.Counter

// newCounters returns factory for tagged counters.
func (c *client) newCounters(name string) countersFunc {
	counters := make(map[string]metrics.Counter)
	mu := &sync.Mutex{}

	return func(tags ...string) metrics.Counter {
		fullName := strings.Join(append([]string{name}, tags...), ".")

		mu.Lock()
		defer mu.Unlock()

		counter, ok := counters[fullName]
		if !ok {
			counter = c.metricsRegistry.GetOrRegister(
				fullName,
				metrics.NewCounter(),
			).(metrics.Counter)
			counters[fullName] = counter
		}
		return counter
	}
}

func (c *client) loopMetrics() {
	var reporters = c.Config.Reporters
	if len(reporters) == 0 {
		c.Logger.Logf("No reporters are configured, metrics won't be reported")
	}

	ep := strings.Split(c.Config.Endpoint, "/")
	enrichReporter := func(reporter Reporter) {
		reporter.AddTags(
			"key:"+fmt.Sprintf("%.6s", c.key),
			"endpoint:"+fmt.Sprintf("%.9s", ep[len(ep)-1]),
		)
		if ctx := c.Config.DefaultContext; ctx != nil {
			if app := ctx.App.Name; app != "" {
				reporter.AddTags("app:" + app)
			}
			if version := ctx.App.Version; version != "" {
				reporter.AddTags("appversion:" + version)
			}
		}
	}
	for _, reporter := range reporters {
		enrichReporter(reporter)
	}
	for {
		select {
		case <-c.quit:
			return
		case <-time.Tick(60 * time.Second):
			c.reportAll("evas.events", reporters)
			c.resetMetrics()
		}
	}
}
