package cmd

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/go-openapi/swag"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
)

type mockDeleteClient struct {
	apigen.ClientWithResponsesInterface
	response *apigen.DeleteObjectsResponse
}

func (m *mockDeleteClient) DeleteObjectsWithResponse(ctx context.Context, repository string, branch string, params *apigen.DeleteObjectsParams, body apigen.DeleteObjectsJSONRequestBody, reqEditors ...apigen.RequestEditorFn) (*apigen.DeleteObjectsResponse, error) {
	return m.response, nil
}

func TestFsRmRecursive_ObjectErrors(t *testing.T) {
	tests := []struct {
		name       string
		response   *apigen.DeleteObjectsResponse
		wantErrors []string
	}{
		{
			name: "with per-object errors",
			response: &apigen.DeleteObjectsResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200: &apigen.ObjectErrorList{
					Errors: []apigen.ObjectError{
						{Path: swag.String("path/to/file1.txt"), StatusCode: 404, Message: "not found"},
						{Path: swag.String("path/to/file2.txt"), StatusCode: 403, Message: "forbidden"},
						{Path: swag.String("path/to/file3.txt"), StatusCode: 429, Message: "slow down"},
					},
				},
			},
			wantErrors: []string{
				"rm path/to/file1.txt: [404] not found",
				"rm path/to/file2.txt: [403] forbidden",
				"rm path/to/file3.txt: [429] slow down",
			},
		},
		{
			name: "no errors",
			response: &apigen.DeleteObjectsResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusOK},
				JSON200:      &apigen.ObjectErrorList{Errors: []apigen.ObjectError{}},
			},
			wantErrors: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockDeleteClient{response: tt.response}

			paths := make(chan string, 1)
			paths <- "some/path"
			close(paths)

			errors := make(chan error, 10)
			var wg sync.WaitGroup
			wg.Add(1)

			deleteObjectWorker(t.Context(), client, "repo", "branch", paths, errors, &wg)

			wg.Wait()
			close(errors)

			var gotErrors []string
			for err := range errors {
				gotErrors = append(gotErrors, err.Error())
			}
			require.Equal(t, tt.wantErrors, gotErrors)
		})
	}
}
