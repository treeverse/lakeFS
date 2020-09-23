package cmd_utils

type ProgressReporter interface {
	Progress() []*Progress
}

type Progress struct {
	Label     string
	Current   int
	Total     int
	Completed bool
}

func (p *Progress) Incr() {
	p.Current++
}

func (p *Progress) Add(n int) {
	p.Current += n
}

func (p *Progress) Set(n int) {
	p.Current = n
}
