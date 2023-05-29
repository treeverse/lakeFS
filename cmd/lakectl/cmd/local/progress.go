package local

import (
	"io"
	"os"
	"time"

	"github.com/jedib0t/go-pretty/v6/progress"
)

type fileWrapper struct {
	file   *os.File
	reader io.Reader
}

func (f fileWrapper) Read(p []byte) (n int, err error) {
	return f.reader.Read(p)
}

func (f fileWrapper) Seek(offset int64, whence int) (int64, error) {
	return f.file.Seek(offset, whence)
}

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

type ProgressSpinner struct {
	t *progress.Tracker
}

func (p *ProgressSpinner) Done() {
	p.t.Increment(100)
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

type ProgressPool struct {
	total int
	pw    progress.Writer
	done  chan bool
}

func (p *ProgressPool) Start() {
	go p.pw.Render()
	go func() {
		t := time.NewTicker(time.Millisecond * 25)
		for {
			select {
			case <-p.done:
				t.Stop()
				return
			case <-t.C:
				//tasksDone := p.pw.LengthDone()
				//p.pw.SetPinnedMessages(fmt.Sprintf("finished %d/%d", tasksDone, p.total))
			}
		}
	}()
}
func (p *ProgressPool) Stop() {
	p.pw.Stop()
	p.done <- true
	// according to examples, give enough time for the last render loop to complete.
	for p.pw.IsRenderInProgress() {
		time.Sleep(time.Millisecond * 5)
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
		Total:   100,
		Units: progress.Units{
			Notation:  "%",
			Formatter: progress.FormatNumber,
		},
	}
	p.pw.AppendTracker(tracker)
	return &ProgressSpinner{t: tracker}
}

func NewProgressPool(total int) *ProgressPool {
	pw := progress.NewWriter()
	pw.SetAutoStop(false) // important
	pw.SetTrackerLength(40)
	pw.SetMessageWidth(60)
	//pw.SetNumTrackersExpected(total)
	pw.SetSortBy(progress.SortByValue)
	pw.SetStyle(progress.StyleDefault)
	pw.SetTrackerPosition(progress.PositionRight)
	pw.SetUpdateFrequency(time.Millisecond * 50)
	pw.Style().Colors = progress.StyleColorsExample
	pw.Style().Options.PercentFormat = "%4.1f%%"
	//pw.Style().Visibility.ETA = true
	//pw.Style().Visibility.ETAOverall = false
	//pw.Style().Visibility.Percentage = true
	//pw.Style().Visibility.Speed = true
	//pw.Style().Visibility.SpeedOverall = true
	//pw.Style().Visibility.Time = true
	//pw.Style().Visibility.TrackerOverall = false
	//pw.Style().Visibility.Value = true
	//pw.Style().Visibility.Pinned = true
	return &ProgressPool{
		pw:    pw,
		total: total,
		done:  make(chan bool),
	}
}
