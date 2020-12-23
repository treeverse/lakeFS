package graveler

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
	parts := modifiersRegexp.FindAllString(string(r), -1)
	if len(parts) == 0 || len(parts[0]) == 0 {
		return ParsedRev{}, ErrInvalidRef
	}
	baseRev := parts[0]
	mods := make([]RevModifier, 0, len(parts)-1)
	for _, part := range parts[1:] {
		mod, err := parseMod(part)
		if err != nil {
			return ParsedRev{}, err
		}
		mods = append(mods, mod)
	}
	return ParsedRev{
		BaseRev:   baseRev,
		Modifiers: mods,
	}, nil
}
