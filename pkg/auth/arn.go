package auth

import (
	"strings"

	"github.com/treeverse/lakefs/pkg/permissions"

	"github.com/treeverse/lakefs/pkg/auth/wildcard"
)

type Arn struct {
	Partition  string
	Service    string
	Region     string
	AccountID  string
	ResourceID string
}

const (
	fieldIndexArn = iota
	fieldIndexPartition
	fieldIndexService
	fieldIndexRegion
	fieldIndexAccount
	fieldIndexResource

	fieldIndexLast = fieldIndexResource
)

func arnParseField(arn *Arn, field string, fieldIndex int) error {
	switch fieldIndex {
	case fieldIndexArn:
		if field != "arn" {
			return ErrInvalidArn
		}
	case fieldIndexPartition:
		if field != "lakefs" {
			return ErrInvalidArn
		}
		arn.Partition = field
	case fieldIndexService:
		if len(field) < 1 {
			return ErrInvalidArn
		}
		arn.Service = field
	case fieldIndexRegion:
		arn.Region = field
	case fieldIndexAccount:
		arn.AccountID = field
	case fieldIndexResource:
		if len(field) < 1 {
			return ErrInvalidArn
		}
		arn.ResourceID = field
	}
	return nil
}

func ParseARN(arnString string) (*Arn, error) {
	a := &Arn{}
	var buf strings.Builder
	currField := 0
	for _, ch := range arnString {
		if ch == ':' || currField == 4 && ch == '/' {
			// collect buffer into current field
			err := arnParseField(a, buf.String(), currField)
			if err != nil {
				return a, err
			}
			// reset and move on
			buf.Reset()
			currField++
		} else {
			buf.WriteRune(ch)
		}
	}
	if buf.Len() > 0 {
		err := arnParseField(a, buf.String(), currField)
		if err != nil {
			return a, err
		}
	}
	if currField < fieldIndexLast {
		return nil, ErrInvalidArn
	}
	return a, nil
}

func ArnMatch(src, dst string) bool {
	if src == permissions.All {
		return true
	}
	source, err := ParseARN(src)
	if err != nil {
		return false
	}
	dest, err := ParseARN(dst)
	if err != nil {
		return false
	}
	if source.Service != dest.Service {
		return false
	}
	if source.Partition != dest.Partition {
		return false
	}
	if source.AccountID != dest.AccountID {
		return false
	}
	// wildcards are allowed for resources only
	if wildcard.Match(source.ResourceID, dest.ResourceID) {
		return true
	}
	return false
}
