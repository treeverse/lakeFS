package operations

import (
	"net/http"
	authmodel "versio-index/auth/model"
	"versio-index/gateway/errors"
	"versio-index/gateway/serde"
)

type ListBuckets struct{}

func (controller *ListBuckets) GetArn() string {
	return "versio:repos:::*"
}

func (controller *ListBuckets) GetIntent() authmodel.Permission_Intent {
	return authmodel.Permission_REPOS_LIST
}

func (controller *ListBuckets) Handle(o *AuthenticatedOperation) {
	repos, err := o.Index.ListRepos(o.ClientId)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// assemble response
	buckets := make([]serde.Bucket, len(repos))
	for i, repo := range repos {
		buckets[i] = serde.Bucket{
			CreationDate: serde.Timestamp(repo.GetCreationDate()),
			Name:         repo.GetRepoId(),
		}
	}
	// get client
	client, err := o.Auth.GetClient(o.ClientId)
	if err != nil {
		o.EncodeError(errors.Codes.ToAPIErr(errors.ErrInternalError))
		return
	}

	// write response
	o.EncodeResponse(serde.ListBucketsOutput{
		Buckets: serde.Buckets{Bucket: buckets},
		Owner: serde.Owner{
			DisplayName: client.GetName(),
			ID:          client.GetId(),
		},
	}, http.StatusOK)

}
