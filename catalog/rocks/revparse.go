package rocks

import (
	"fmt"
	"regexp"
	"strconv"
)

var (
	hashRegexp      = regexp.MustCompile("^[a-fA-F0-9]{1,64}$")
	modifiersRegexp = regexp.MustCompile("(^|[~^])[^^~]*")
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
			return RevModifier{}, fmt.Errorf("could not parse modifier %s: %w", buf, ErrInvalidRef)
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
	parts := modifiersRegexp.FindAllString(string(r), -1)
	p.BaseRev = parts[0]
	if p.BaseRev == "" {
		return p, ErrInvalidRef
	}
	for _, part := range parts[1:] {
		mod, err := parseMod(part)
		if err != nil {
			return p, err
		}
		p.Modifiers = append(p.Modifiers, mod)
	}
	return p, nil
}
