package auth

import (
	"strings"
)

// arn:${Partition}:s3:::${BucketName}
// e.g. arn:aws:s3:::myrepo
// in our case, arn:versio:repos:::myrepo

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

func ResolveARN(arnString string, context map[string]string) string {
	// parse the arn to extract placeholders and locations
	inTag := false
	openTag := '{'
	closeTag := '}'
	type tag struct {
		startIndex int
		endIndex   int
		key        string
	}
	tags := make([]tag, 0)
	currentTag := tag{}
	for i, ch := range arnString {
		if ch == openTag && !inTag {
			currentTag = tag{startIndex: i}
			inTag = true
		} else if ch == closeTag && inTag {
			currentTag.endIndex = i
			tags = append(tags, currentTag)
			currentTag = tag{}
			inTag = false
		} else if inTag {
			currentTag.key = currentTag.key + string(ch)
		}
	}
	// replace placeholders with values found in the context
	lengthCompensation := 0
	for _, tag := range tags {
		value := context[tag.key]
		prefix := arnString[0 : tag.startIndex+lengthCompensation]
		suffix := arnString[tag.endIndex+1+lengthCompensation:]
		arnString = prefix + value + suffix
		// now account for changes in length of string?!
		lengthCompensation = lengthCompensation + len(value) - (tag.endIndex - tag.startIndex + 1) // string is now lengthCompensation longer than it was.
	}
	return arnString
}

func ArnMatch(src, dst string) bool {
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
	if strings.EqualFold(source.ResourceId, "*") || source.ResourceId == dest.ResourceId {
		return true
	}
	return false
}
