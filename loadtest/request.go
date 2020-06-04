package loadtest

import vegeta "github.com/tsenart/vegeta/v12/lib"

type Request struct {
	Target vegeta.Target
	Type   string
}

func NewRequest(tgt vegeta.Target, typ string) Request {
	return Request{
		Target: tgt,
		Type:   typ,
	}
}
