package auth

import (
	"strings"

	"github.com/treeverse/lakefs/pkg/auth/wildcard"
	"github.com/treeverse/lakefs/pkg/permissions"
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

	numFieldIndexes
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
	// BUG(ozkatz): This parser does not handle resource types.  Handling resource types is
	//    subtle: they may be separated from resource IDs by a colon OR by a slash.  For an
	//    example of a resource type, see ECS[1] (uses only slash separators).  That colons
	//    are an acceptable separator appears in [2], so a workaround to this limitation is
	//    to use a slash.
	//
	//    [1] https://docs.aws.amazon.com/AmazonECS/latest/developerguide/security_iam_service-with-iam.html#security_iam_service-with-iam-id-based-policies-resources
	//    [2] https://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#arns-syntax
	parts := strings.SplitN(arnString, ":", numFieldIndexes)

	if len(parts) < numFieldIndexes {
		return nil, ErrInvalidArn
	}

	arn := &Arn{}

	for currField, part := range parts {
		err := arnParseField(arn, part, currField)
		if err != nil {
			return arn, err
		}
	}

	return arn, nil
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
