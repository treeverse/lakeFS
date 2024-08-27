package local

import (
	"io"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
)

const (
	progressTrackerLength          = 20
	progressTrackerWidth           = 40
	progressTrackerDone            = 100
	progressTrackerUpdateFrequency = 50 * time.Millisecond
)

type ProgressUpdaterReader struct {
	r io.Reader
	t *progress.Tracker
}

func (pu *ProgressUpdaterReader) Read(p []byte) (n int, err error) {
	n, err = pu.r.Read(p)
	pu.t.Increment(int64(n))
	if err == io.EOF {
		pu.t.MarkAsDone()
	} else if err != nil {
		pu.t.IncrementWithError(int64(n))
	}
	return
}

type ProgressUpdater struct {
	t *progress.Tracker
}

func (p *ProgressUpdater) Reader(reader io.Reader) io.Reader {
	return &ProgressUpdaterReader{
		r: reader,
		t: p.t,
	}
}

func (p *ProgressUpdater) Done() {
	p.t.MarkAsDone()
}

func (p *ProgressUpdater) Error() {
	p.t.MarkAsErrored()
}

type ProgressSpinner struct {
	t *progress.Tracker
}

func (p *ProgressSpinner) Error() {
	p.t.MarkAsErrored()
}

func (p *ProgressSpinner) Done() {
	p.t.MarkAsDone()
}

type ProgressPool struct {
	pw   progress.Writer
	done chan bool
}

func (p *ProgressPool) Start() {
	const tickerDuration = 25 * time.Millisecond
	go p.pw.Render()
	go func() {
		t := time.NewTicker(tickerDuration)
		for {
			select {
			case <-p.done:
				t.Stop()
				return
			case <-t.C:
			}
		}
	}()
}
func (p *ProgressPool) Stop() {
	p.pw.Stop()
	p.done <- true
	// according to examples, give enough time for the last render loop to complete.
	for p.pw.IsRenderInProgress() {
		const renderSleep = 5 * time.Millisecond
		time.Sleep(renderSleep)
	}
}

func (p *ProgressPool) AddReader(name string, sizeBytes int64) *ProgressUpdater {
	tracker := &progress.Tracker{
		Message: name,
		Total:   sizeBytes,
		Units:   progress.UnitsBytes,
	}
	p.pw.AppendTracker(tracker)
	return &ProgressUpdater{t: tracker}
}

func (p *ProgressPool) AddSpinner(name string) *ProgressSpinner {
	tracker := &progress.Tracker{
		Message: name,
		Total:   progressTrackerDone,
		Units: progress.Units{
			Notation:  "%",
			Formatter: progress.FormatNumber,
		},
	}
	p.pw.AppendTracker(tracker)
	return &ProgressSpinner{t: tracker}
}

func NewProgressPool() *ProgressPool {
	pw := progress.NewWriter()
	pw.SetAutoStop(false) // important
	pw.SetTrackerLength(progressTrackerLength)
	pw.SetMessageLength(progressTrackerWidth)
	pw.SetSortBy(progress.SortByValue)
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.SetUpdateFrequency(progressTrackerUpdateFrequency)
	pw.Style().Colors = progress.StyleColorsExample
	pw.Style().Options.PercentFormat = "%4.1f%%"

	return &ProgressPool{
		pw:   pw,
		done: make(chan bool),
	}
}

type fileWrapper struct {
	file   io.Seeker
	reader io.Reader
}

func (f fileWrapper) Read(p []byte) (n int, err error) {
	return f.reader.Read(p)
}

func (f fileWrapper) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}
