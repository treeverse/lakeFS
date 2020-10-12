package cmdutils

import (
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/text"

	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"
)

const (
	progressCounterFormat         = "%d / %d "
	spinnerCounterFormat          = "%d "
	spinnerNoCounterFormat        = ""
	progressSuffix                = "]"
	progressPrefix                = "["
	progressBarWidth              = 60
	progressBarNameColumnWidth    = 40
	progressBarCounterColumnWidth = 20
	progressBarStyle              = " =>- <"
	progressRefreshRate           = 50 * time.Millisecond
)

const (
	Bar = iota
	Spinner
	SpinnerNoCounter
)

type ProgressReporter interface {
	Progress() []*Progress
}

type Progress struct {
	label        string
	current      *int64
	total        *int64
	completed    bool
	active       bool
	progressType int
}

type MultiBar struct {
	reporter ProgressReporter
	mpb      *mpb.Progress
	mpbBars  map[string]*mpb.Bar
	ticker   *time.Ticker
}

func NewProgress(label string, progressType int) *Progress {
	total := int64(0)
	if progressType == Spinner || progressType == SpinnerNoCounter {
		total = int64(-1)
	}
	return &Progress{
		label:        label,
		current:      new(int64),
		total:        &total,
		progressType: progressType,
	}
}

func NewActiveProgress(label string, progressType int) *Progress {
	res := NewProgress(label, progressType)
	res.Activate()
	return res
}

func (p *Progress) Label() string {
	return p.label
}

func (p *Progress) Current() int64 {
	return atomic.LoadInt64(p.current)
}

func (p *Progress) Total() int64 {
	return atomic.LoadInt64(p.total)
}

func (p *Progress) Incr() {
	atomic.AddInt64(p.current, 1)
}

func (p *Progress) Add(n int64) {
	atomic.AddInt64(p.current, n)
}

func (p *Progress) SetCurrent(n int64) {
	atomic.StoreInt64(p.current, n)
}

func (p *Progress) SetTotal(n int64) {
	atomic.StoreInt64(p.total, n)
}

func (p *Progress) Completed() bool {
	return p.completed
}

func (p *Progress) SetCompleted(completed bool) {
	p.completed = completed
}

func (p *Progress) Activate() {
	p.active = true
}

func NewMultiBar(r ProgressReporter) *MultiBar {
	ticker := time.NewTicker(progressRefreshRate)
	m := mpb.New(mpb.WithWidth(progressBarWidth))
	bars := make(map[string]*mpb.Bar)
	return &MultiBar{reporter: r, mpbBars: bars, mpb: m, ticker: ticker}
}

func (b *MultiBar) Start() {
	go func() {
		for range b.ticker.C {
			b.refresh(false)
		}
	}()
}

func (b *MultiBar) Stop() {
	b.refresh(true)
	b.ticker.Stop()
	b.mpb.Wait()
}

func (b *MultiBar) refresh(isStop bool) {
	progress := b.reporter.Progress()
	for _, p := range progress {
		if p == nil || !p.active {
			continue
		}
		total := p.Total()
		bar, ok := b.mpbBars[p.label]
		if !ok {
			bar = createBar(b.mpb, p, total)
			b.mpbBars[p.label] = bar
		}
		if !isStop {
			bar.SetTotal(total, p.Completed())
		} else {
			bar.SetTotal(p.Current(), true)
		}
		bar.SetCurrent(p.Current())
	}
}

func createBar(m *mpb.Progress, p *Progress, total int64) *mpb.Bar {
	var bar *mpb.Bar
	labelDecorator := decor.Name(p.label, decor.WC{W: progressBarNameColumnWidth, C: decor.DidentRight})
	suffixOption := mpb.AppendDecorators(decor.OnComplete(decor.Name(progressSuffix), text.FgGreen.Sprintf(" done")))
	if p.progressType == Spinner || p.progressType == SpinnerNoCounter {
		// unknown total, render a spinner
		var counterDecorator decor.Decorator
		if p.progressType == Spinner {
			counterDecorator = decor.CurrentNoUnit(spinnerCounterFormat, decor.WC{W: progressBarCounterColumnWidth})
		} else {
			counterDecorator = decor.Name(spinnerNoCounterFormat, decor.WC{W: progressBarCounterColumnWidth})
		}
		bar = m.AddSpinner(total, mpb.SpinnerOnMiddle, mpb.BarFillerClearOnComplete(), suffixOption, mpb.PrependDecorators(labelDecorator, counterDecorator, decor.OnComplete(decor.Name(progressPrefix), "")))
	} else { // type == Bar
		bar = m.AddBar(total, mpb.BarStyle(progressBarStyle), mpb.BarFillerClearOnComplete(), suffixOption,
			mpb.PrependDecorators(labelDecorator,
				decor.CountersNoUnit(progressCounterFormat, decor.WC{W: progressBarCounterColumnWidth}),
				decor.OnComplete(decor.Name(progressPrefix), "")))
	}
	return bar
}
