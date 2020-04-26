package stats

import (
	"context"
	"time"

	"github.com/treeverse/lakefs/logging"
)

const FlushErrorTimeout = time.Millisecond * 100

type Metric struct {
	Class string `json:"class"`
	Name  string `json:"name"`
	Value uint64 `json:"value"`
}

type InputEvent struct {
	Email     string   `json:"email"`
	ProcessId string   `json:"process_id"`
	Time      string   `json:"time"`
	Metrics   []Metric `json:"metrics"`
}

type TimeFn func() time.Time

type Dispatcher struct {
	ctx           context.Context
	flushInterval time.Duration
	timeFn        TimeFn
	collector     *CallHomeCollector
	sender        Sender

	procId string
	userId string
}

func NewDispatcher(ctx context.Context, flushInterval time.Duration, sender Sender, procId, userId string) *Dispatcher {
	return &Dispatcher{
		ctx:           ctx,
		flushInterval: flushInterval,
		timeFn:        time.Now,
		collector:     NewCallHomeCollector(),
		sender:        sender,
		procId:        procId,
		userId:        userId,
	}
}

func makeMetrics(counters keyIndex) []Metric {
	metrics := make([]Metric, len(counters))
	i := 0
	for k, v := range counters {
		metrics[i] = Metric{
			Class: k.class,
			Name:  k.name,
			Value: v,
		}
		i++
	}
	return metrics
}

func (d *Dispatcher) send(metrics []Metric) error {
	// serialize and write the event
	if len(metrics) == 0 {
		return nil
	}
	return d.sender.Send(InputEvent{
		Email:     d.userId,
		ProcessId: d.procId,
		Time:      d.timeFn().Format(time.RFC3339),
		Metrics:   metrics,
	})
}

func (d *Dispatcher) flush(metrics []Metric) {
	err := d.send(metrics)
	if err != nil {
		logging.Default().
			WithError(err).
			WithField("service", "log_dispatcher").
			Debug("could not send stats")
	}
}

func (d *Dispatcher) Collect(class, action string) {
	d.collector.Collect(class, action)
}

func (d *Dispatcher) Run(done chan bool) {
	go d.collector.Run()
	ticker := time.NewTicker(d.flushInterval)
	for {
		select {
		case <-d.ctx.Done():
			ticker.Stop()
			counters := d.collector.Drain()
			d.flush(makeMetrics(counters))
			done <- true
			return
		case <-ticker.C:
			counters := d.collector.Flush()
			d.flush(makeMetrics(counters))
		}
	}
}
