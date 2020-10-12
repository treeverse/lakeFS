package cmdutils

import (
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/text"

	"github.com/vbauerster/mpb/v5"
	"github.com/vbauerster/mpb/v5/decor"
)

const (
	progressCounterFormat         = "%d / %d ["
	spinnerCounterFormat          = "%d ["
	progressSuffix                = "]"
	progressBarWidth              = 60
	progressBarNameColumnWidth    = 40
	progressBarCounterColumnWidth = 20
	progressBarStyle              = " =>- <"
	progressRefreshRate           = 50 * time.Millisecond
)

type ProgressReporter interface {
	Progress() []*Progress
}

type Progress struct {
	label     string
	current   *int64
	total     *int64
	completed bool
}

type MultiBar struct {
	reporter ProgressReporter
	mpb      *mpb.Progress
	mpbBars  map[string]*mpb.Bar
	ticker   *time.Ticker
}

func NewProgress(label string, total int64) *Progress {
	return &Progress{
		label:   label,
		current: new(int64),
		total:   &total,
	}
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

func (b *MultiBar) refresh(isCompleted bool) {
	progress := b.reporter.Progress()
	for _, p := range progress {
		total := p.Total()
		bar, ok := b.mpbBars[p.label]
		if !ok {
			bar = createBar(b.mpb, p, total)
			b.mpbBars[p.label] = bar
		}
		if !isCompleted {
			bar.SetTotal(total, false)
		} else {
			p.SetCompleted(true)
			bar.SetTotal(p.Current(), true)
		}
		bar.SetCurrent(p.Current())
	}
}

func createBar(m *mpb.Progress, p *Progress, total int64) *mpb.Bar {
	var bar *mpb.Bar
	isSpinner := false
	if total < 0 {
		isSpinner = true
	}
	labelDecorator := decor.Name(p.label, decor.WC{W: progressBarNameColumnWidth, C: decor.DidentRight})
	suffixOption := mpb.AppendDecorators(decor.Name(progressSuffix), decor.Any(func(statistics decor.Statistics) string {
		if p.Completed() {
			return text.FgGreen.Sprintf(" done")
		}
		return ""
	}))
	if isSpinner {
		// unknown total, render a spinner
		bar = m.AddSpinner(total, mpb.SpinnerOnMiddle, suffixOption,
			mpb.PrependDecorators(labelDecorator,
				decor.CurrentNoUnit(spinnerCounterFormat, decor.WC{W: progressBarCounterColumnWidth})))
	} else {
		bar = m.AddBar(total, mpb.BarStyle(progressBarStyle), suffixOption,
			mpb.PrependDecorators(labelDecorator,
				decor.CountersNoUnit(progressCounterFormat, decor.WC{W: progressBarCounterColumnWidth})))
	}
	return bar
}
