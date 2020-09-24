package cmdutils

import (
	"sync/atomic"
	"time"

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
	label   string
	current *int64
	total   *int64
}

type MultiBar struct {
	reporter ProgressReporter
	mpb      *mpb.Progress
	mpbBars  map[string]*mpb.Bar
	done     chan bool
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

func StartMultiBar(r ProgressReporter) *MultiBar {
	ticker := time.NewTicker(progressRefreshRate)
	m := mpb.New(mpb.WithWidth(progressBarWidth))
	done := make(chan bool)
	bars := make(map[string]*mpb.Bar)
	multi := &MultiBar{reporter: r, mpbBars: bars, mpb: m, done: done}
	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				multi.Refresh(false)
			}
		}
	}()
	return multi
}

func (b *MultiBar) Finish() {
	close(b.done)
}

func (b *MultiBar) Refresh(isCompleted bool) {
	progress := b.reporter.Progress()
	for _, p := range progress {
		total := atomic.LoadInt64(p.total)
		isSpinner := false
		if total <= 0 {
			isSpinner = true
			total = atomic.LoadInt64(p.current)
		}
		bar, ok := b.mpbBars[p.label]
		if !ok {
			labelDecorator := decor.Name(p.label, decor.WC{W: progressBarNameColumnWidth, C: decor.DidentRight})
			suffixOption := mpb.AppendDecorators(decor.Name(progressSuffix))
			if isSpinner {
				// unknown total, render a spinner
				bar = b.mpb.AddSpinner(total, mpb.SpinnerOnMiddle, suffixOption,
					mpb.PrependDecorators(labelDecorator,
						decor.CurrentNoUnit(spinnerCounterFormat, decor.WC{W: progressBarCounterColumnWidth})))
			} else {
				bar = b.mpb.AddBar(total, mpb.BarStyle(progressBarStyle), suffixOption,
					mpb.PrependDecorators(labelDecorator,
						decor.CountersNoUnit(progressCounterFormat, decor.WC{W: progressBarCounterColumnWidth})))
			}
			b.mpbBars[p.label] = bar
		}
		bar.SetTotal(total, isCompleted)
		if !isCompleted {
			bar.SetCurrent(atomic.LoadInt64(p.current))
		} else {
			bar.SetCurrent(total)
		}
	}
}
