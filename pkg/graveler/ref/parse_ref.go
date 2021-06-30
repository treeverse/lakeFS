package ref

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/treeverse/lakefs/pkg/graveler"
)

var modifiersRegexp = regexp.MustCompile("(^|[~^@$])[^^~@$]*")

func parseRefModifier(buf string) (graveler.RefModifier, error) {
	amount := 1
	var err error
	var typ graveler.RefModType
	switch buf[0] {
	case '~':
		typ = graveler.RefModTypeTilde
	case '^':
		typ = graveler.RefModTypeCaret
	case '$':
		typ = graveler.RefModTypeDollar
		if len(buf) > 1 {
			return graveler.RefModifier{}, graveler.ErrInvalidRef
		}
	case '@':
		typ = graveler.RefModTypeAt
		if len(buf) > 1 {
			return graveler.RefModifier{}, graveler.ErrInvalidRef
		}
	default:
		return graveler.RefModifier{}, graveler.ErrInvalidRef
	}

	if len(buf) > 1 {
		amount, err = strconv.Atoi(buf[1:])
		if err != nil {
			return graveler.RefModifier{}, fmt.Errorf("could not parse modifier %s: %w", buf, graveler.ErrInvalidRef)
		}
	}

	return graveler.RefModifier{
		Type:  typ,
		Value: amount,
	}, nil
}

func ParseRef(r graveler.Ref) (graveler.RawRef, error) {
	ref := string(r)
	parts := modifiersRegexp.FindAllString(ref, -1)
	if len(parts) == 0 || len(parts[0]) == 0 {
		return graveler.RawRef{}, graveler.ErrInvalidRef
	}
	baseRef := parts[0]
	mods := make([]graveler.RefModifier, 0, len(parts)-1)
	for _, part := range parts[1:] {
		mod, err := parseRefModifier(part)
		if err != nil {
			return graveler.RawRef{}, err
		}
		mods = append(mods, mod)
	}
	return graveler.RawRef{
		BaseRef:   baseRef,
		Modifiers: mods,
	}, nil
}
