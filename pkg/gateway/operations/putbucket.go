package operations

import (
	"net/http"

	gatewayerrors "github.com/treeverse/lakefs/pkg/gateway/errors"
	"github.com/treeverse/lakefs/pkg/permissions"
)

// PutBucket handles S3 Create Bucket operations.  It does *not* actually
// create new repos (there is not enough information in the S3 request to
// create a new repo), but *does* detect whether the repo already exists.
type PutBucket struct{}

func (controller *PutBucket) RequiredPermissions(_ *http.Request, repoID string) (permissions.Node, error) {
	return permissions.Node{
		Permission: permissions.Permission{
			// Mimic S3, which requires s3:CreateBucket to call
			// create-bucket, even if we only want to receive
			// 409.
			Action:   permissions.CreateRepositoryAction,
			Resource: permissions.RepoArn(repoID),
		},
	}, nil
}

func (controller *PutBucket) Handle(w http.ResponseWriter, req *http.Request, o *RepoOperation) {
	if o.HandleUnsupported(w, req, "cors", "metrics", "website", "logging", "accelerate",
		"requestPayment", "acl", "publicAccessBlock", "ownershipControls", "intelligent-tiering", "analytics",
		"lifecycle", "replication", "encryption", "policy", "object-lock", "tagging", "versioning") {
		return
	}

	o.Incr("put_repo", o.Principal, o.Repository.Name, "")
	o.EncodeError(w, req, nil, gatewayerrors.ErrBucketAlreadyExists.ToAPIErr())
}
