package auth

import (
	"strings"

	"github.com/treeverse/lakefs/permissions"

	"github.com/treeverse/lakefs/auth/wildcard"
)

// arn:${Partition}:s3:::${BucketName}
// e.g. arn:aws:s3:::myrepo
// in our case, arn:lakefs:repos:::myrepo

const (
	ArnPartition   = "arn"
	ArnServiceName = "lakefs"
	ArnWildcardAll = "*"
	ArnWildcardOne = "?"
)

type Arn struct {
	Partition  string
	Service    string
	Region     string
	AccountId  string
	ResourceId string
}

func arnParseField(arn *Arn, field string, fieldIndex int) error {
	switch fieldIndex {
	case 0:
		if !strings.EqualFold(field, "arn") {
			return ErrInvalidArn
		}
	case 1:
		if !strings.EqualFold(field, "lakefs") {
			return ErrInvalidArn
		}
		arn.Partition = field
	case 2:
		if len(field) < 1 {
			return ErrInvalidArn
		}
		arn.Service = field
	case 3:
		arn.Region = field
	case 4:
		arn.AccountId = field
	case 5:
		if len(field) < 1 {
			return ErrInvalidArn
		}
		arn.ResourceId = field
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
	if currField < 5 {
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
	if source.AccountId != dest.AccountId {
		return false
	}
	// wildcards are allowed for resources only
	if wildcard.Match(source.ResourceId, dest.ResourceId) {
		return true
	}
	return false
}
