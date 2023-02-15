package health

import (
	"context"

	"github.com/treeverse/lakefs/cmd/lakectl/cmd"
)

type HealthStatus struct {
	lakeFS string `json:"lakeFS"`
}

type Service interface {
	Health(ctx context.Context) HealthStatus
}

type service struct{}

func New() Service {
	return &service{}
}

func (s *service) Health(ctx context.Context) HealthStatus {
	err := cmd.ListRepositoriesAndAnalyze(ctx)

	return HealthStatus{
		lakeFS: err.Error(),
	}
}
