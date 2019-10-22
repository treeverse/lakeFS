package auth

import (
	"strings"

	"golang.org/x/xerrors"
)

// arn:${Partition}:s3:::${BucketName}
// e.g. arn:aws:s3:::myrepo
// in our case, arn:versio:repos:::myrepo

var ErrInvalidArn = xerrors.New("invalid ARN")

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
		if !strings.EqualFold(field, "versio") {
			return ErrInvalidArn
		}
		arn.Partition = field
	case 2:
		if len(field) < 1 {
			return ErrInvalidArn
		}
		arn.Service = field
	case 3:
		if len(field) != 0 {
			return ErrInvalidArn
		}
		arn.Region = field
	case 4:
		if len(field) != 0 {
			return ErrInvalidArn
		}
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
		if ch == ':' || currField == 5 && ch == '/' {
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
	return a, nil
}

func ArnMatch(a, b string) bool {
	source, err := ParseARN(a)
	if err != nil {
		return false
	}
	dest, err := ParseARN(b)
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
	if strings.EqualFold(source.ResourceId, "*") || source.ResourceId == dest.ResourceId {
		return true
	}
	return false
}
