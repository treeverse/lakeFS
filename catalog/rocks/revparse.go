package rocks

import (
	"regexp"
	"strconv"
)

var (
	hashRegexp = regexp.MustCompile("^[a-fA-F0-9]{1,64}$")
)

type RevModType uint8

const (
	RevModTypeTilde RevModType = iota
	RevModTypeCaret
)

func isAHash(part string) bool {
	return hashRegexp.MatchString(part)
}

type ParsedRev struct {
	BaseRev   string
	Modifiers []RevModifier
}

type RevModifier struct {
	Type  RevModType
	Value int
}

func parseMod(buf string) (RevModifier, error) {
	amount := 1
	var err error
	if len(buf) > 1 {
		amount, err = strconv.Atoi(buf[1:])
		if err != nil {
			return RevModifier{}, ErrInvalidRef
		}
	}
	var typ RevModType
	switch buf[0] {
	case '~':
		typ = RevModTypeTilde
	case '^':
		typ = RevModTypeCaret
	default:
		return RevModifier{}, ErrInvalidRef
	}

	return RevModifier{
		Type:  typ,
		Value: amount,
	}, nil
}

func RevParse(r Ref) (ParsedRev, error) {
	p := ParsedRev{
		Modifiers: make([]RevModifier, 0),
	}
	buf := ""
	statusInBase := true
	for _, ch := range r {
		switch ch {
		case '~', '^':
			// this is a modifier, finalize previous state. error if no previous state
			if statusInBase {
				if buf == "" {
					// no base found
					return p, ErrInvalidRef
				}
				p.BaseRev = buf
				statusInBase = false
				buf = string(ch)
			} else {
				// starting a new modifier while in another modifier
				mod, err := parseMod(buf)
				if err != nil {
					return p, err
				}
				p.Modifiers = append(p.Modifiers, mod)
				buf = string(ch)
			}
		default:
			buf += string(ch)
		}
	}
	// finalize previous state
	if statusInBase {
		if buf == "" {
			return p, ErrInvalidRef
		}
		p.BaseRev = buf
		return p, nil
	}

	// was in a modifier
	mod, err := parseMod(buf)
	if err != nil {
		return p, err
	}
	p.Modifiers = append(p.Modifiers, mod)
	return p, nil
}
