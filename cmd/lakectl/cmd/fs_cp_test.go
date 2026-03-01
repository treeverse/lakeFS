package cmd

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/api/apigen"
	"github.com/treeverse/lakefs/pkg/uri"
)

// mockCopyClient implements the required interface for copy/move testing
type mockCopyClient struct {
	apigen.ClientWithResponsesInterface
	copyResponses   map[string]*apigen.CopyObjectResponse
	deleteResponses map[string]*apigen.DeleteObjectResponse
	listResponse    *apigen.ListObjectsResponse
	copyCalls       []copyCall
	deleteCalls     []string
}

type copyCall struct {
	srcPath  string
	destPath string
	srcRef   string
	destRef  string
}

func (m *mockCopyClient) CopyObjectWithResponse(ctx context.Context, repository string, branch string, params *apigen.CopyObjectParams, body apigen.CopyObjectJSONRequestBody, reqEditors ...apigen.RequestEditorFn) (*apigen.CopyObjectResponse, error) {
	srcRef := ""
	if body.SrcRef != nil {
		srcRef = *body.SrcRef
	}
	m.copyCalls = append(m.copyCalls, copyCall{
		srcPath:  body.SrcPath,
		destPath: params.DestPath,
		srcRef:   srcRef,
		destRef:  branch,
	})

	key := fmt.Sprintf("%s:%s", body.SrcPath, params.DestPath)
	if resp, ok := m.copyResponses[key]; ok {
		return resp, nil
	}
	// Default success response
	return &apigen.CopyObjectResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusCreated},
		JSON201: &apigen.ObjectStats{
			Path:     params.DestPath,
			Checksum: "abc123",
		},
	}, nil
}

func (m *mockCopyClient) DeleteObjectWithResponse(ctx context.Context, repository string, branch string, params *apigen.DeleteObjectParams, reqEditors ...apigen.RequestEditorFn) (*apigen.DeleteObjectResponse, error) {
	m.deleteCalls = append(m.deleteCalls, params.Path)

	if resp, ok := m.deleteResponses[params.Path]; ok {
		return resp, nil
	}
	return &apigen.DeleteObjectResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
	}, nil
}

func (m *mockCopyClient) DeleteObjectsWithResponse(ctx context.Context, repository string, branch string, params *apigen.DeleteObjectsParams, body apigen.DeleteObjectsJSONRequestBody, reqEditors ...apigen.RequestEditorFn) (*apigen.DeleteObjectsResponse, error) {
	m.deleteCalls = append(m.deleteCalls, body.Paths...)
	return &apigen.DeleteObjectsResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200:      &apigen.ObjectErrorList{},
	}, nil
}

func (m *mockCopyClient) ListObjectsWithResponse(ctx context.Context, repository string, ref string, params *apigen.ListObjectsParams, reqEditors ...apigen.RequestEditorFn) (*apigen.ListObjectsResponse, error) {
	if m.listResponse != nil {
		return m.listResponse, nil
	}
	return &apigen.ListObjectsResponse{
		HTTPResponse: &http.Response{StatusCode: http.StatusOK},
		JSON200: &apigen.ObjectStatsList{
			Results:    []apigen.ObjectStats{},
			Pagination: apigen.Pagination{HasMore: false},
		},
	}, nil
}

func TestCopyObject(t *testing.T) {
	ctx := context.Background()

	t.Run("single_copy_success", func(t *testing.T) {
		client := &mockCopyClient{
			copyResponses: make(map[string]*apigen.CopyObjectResponse),
		}

		srcPath := "path/to/source.txt"
		destPath := "path/to/dest.txt"
		srcURI := &uri.URI{Repository: "repo", Ref: "main", Path: &srcPath}
		destURI := &uri.URI{Repository: "repo", Ref: "main", Path: &destPath}

		stat, err := copyObject(ctx, client, srcURI, destURI)
		require.NoError(t, err)
		require.NotNil(t, stat)
		require.Equal(t, destPath, stat.Path)

		require.Len(t, client.copyCalls, 1)
		require.Equal(t, srcPath, client.copyCalls[0].srcPath)
		require.Equal(t, destPath, client.copyCalls[0].destPath)
	})

	t.Run("copy_different_refs", func(t *testing.T) {
		client := &mockCopyClient{
			copyResponses: make(map[string]*apigen.CopyObjectResponse),
		}

		srcPath := "path/to/source.txt"
		destPath := "path/to/dest.txt"
		srcURI := &uri.URI{Repository: "repo", Ref: "feature", Path: &srcPath}
		destURI := &uri.URI{Repository: "repo", Ref: "main", Path: &destPath}

		stat, err := copyObject(ctx, client, srcURI, destURI)
		require.NoError(t, err)
		require.NotNil(t, stat)

		require.Len(t, client.copyCalls, 1)
		require.Equal(t, "feature", client.copyCalls[0].srcRef)
		require.Equal(t, "main", client.copyCalls[0].destRef)
	})

	t.Run("copy_failure", func(t *testing.T) {
		client := &mockCopyClient{
			copyResponses: map[string]*apigen.CopyObjectResponse{
				"src.txt:dest.txt": {
					HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
					JSON201:      nil,
					Body:         []byte(`{"message": "not found"}`),
				},
			},
		}

		srcPath := "src.txt"
		destPath := "dest.txt"
		srcURI := &uri.URI{Repository: "repo", Ref: "main", Path: &srcPath}
		destURI := &uri.URI{Repository: "repo", Ref: "main", Path: &destPath}

		stat, err := copyObject(ctx, client, srcURI, destURI)
		require.Error(t, err)
		require.Nil(t, stat)
	})
}

func TestMoveObject(t *testing.T) {
	ctx := context.Background()

	t.Run("single_move_success", func(t *testing.T) {
		client := &mockCopyClient{
			copyResponses:   make(map[string]*apigen.CopyObjectResponse),
			deleteResponses: make(map[string]*apigen.DeleteObjectResponse),
		}

		srcPath := "path/to/source.txt"
		destPath := "path/to/dest.txt"
		srcURI := &uri.URI{Repository: "repo", Ref: "main", Path: &srcPath}
		destURI := &uri.URI{Repository: "repo", Ref: "main", Path: &destPath}

		stat, err := moveObject(ctx, client, srcURI, destURI)
		require.NoError(t, err)
		require.NotNil(t, stat)
		require.Equal(t, destPath, stat.Path)

		// Verify copy was called
		require.Len(t, client.copyCalls, 1)
		require.Equal(t, srcPath, client.copyCalls[0].srcPath)
		require.Equal(t, destPath, client.copyCalls[0].destPath)

		// Verify delete was called
		require.Len(t, client.deleteCalls, 1)
		require.Equal(t, srcPath, client.deleteCalls[0])
	})

	t.Run("move_copy_fails", func(t *testing.T) {
		client := &mockCopyClient{
			copyResponses: map[string]*apigen.CopyObjectResponse{
				"src.txt:dest.txt": {
					HTTPResponse: &http.Response{StatusCode: http.StatusNotFound},
					JSON201:      nil,
					Body:         []byte(`{"message": "not found"}`),
				},
			},
			deleteResponses: make(map[string]*apigen.DeleteObjectResponse),
		}

		srcPath := "src.txt"
		destPath := "dest.txt"
		srcURI := &uri.URI{Repository: "repo", Ref: "main", Path: &srcPath}
		destURI := &uri.URI{Repository: "repo", Ref: "main", Path: &destPath}

		stat, err := moveObject(ctx, client, srcURI, destURI)
		require.Error(t, err)
		require.Nil(t, stat)

		// Verify delete was NOT called since copy failed
		require.Empty(t, client.deleteCalls)
	})

	t.Run("move_delete_fails", func(t *testing.T) {
		client := &mockCopyClient{
			copyResponses: make(map[string]*apigen.CopyObjectResponse),
			deleteResponses: map[string]*apigen.DeleteObjectResponse{
				"src.txt": {
					HTTPResponse: &http.Response{StatusCode: http.StatusForbidden},
					Body:         []byte(`{"message": "forbidden"}`),
				},
			},
		}

		srcPath := "src.txt"
		destPath := "dest.txt"
		srcURI := &uri.URI{Repository: "repo", Ref: "main", Path: &srcPath}
		destURI := &uri.URI{Repository: "repo", Ref: "main", Path: &destPath}

		stat, err := moveObject(ctx, client, srcURI, destURI)
		require.Error(t, err)
		require.Nil(t, stat)
		require.Contains(t, err.Error(), "delete source after copy")

		// Verify both copy and delete were called
		require.Len(t, client.copyCalls, 1)
		require.Len(t, client.deleteCalls, 1)
	})
}

func TestDeleteObjectsBatch(t *testing.T) {
	ctx := context.Background()

	t.Run("single_batch", func(t *testing.T) {
		client := &mockCopyClient{}

		paths := []string{"file1.txt", "file2.txt", "file3.txt"}
		errs := deleteObjectsBatch(ctx, client, "repo", "main", paths)
		require.Empty(t, errs)
		require.Equal(t, paths, client.deleteCalls)
	})

	t.Run("multiple_batches", func(t *testing.T) {
		client := &mockCopyClient{}

		// Create more than deleteChunkSize paths
		paths := make([]string, deleteChunkSize+10)
		for i := range paths {
			paths[i] = fmt.Sprintf("file%d.txt", i)
		}

		errs := deleteObjectsBatch(ctx, client, "repo", "main", paths)
		require.Empty(t, errs)
		require.Equal(t, paths, client.deleteCalls)
	})

	t.Run("empty_paths", func(t *testing.T) {
		client := &mockCopyClient{}

		errs := deleteObjectsBatch(ctx, client, "repo", "main", []string{})
		require.Empty(t, errs)
		require.Empty(t, client.deleteCalls)
	})
}

// Implement missing interface methods to satisfy the interface
func (m *mockCopyClient) GetConfigWithResponse(ctx context.Context, reqEditors ...apigen.RequestEditorFn) (*apigen.GetConfigResponse, error) {
	return nil, nil
}

func (m *mockCopyClient) HealthCheckWithResponse(ctx context.Context, reqEditors ...apigen.RequestEditorFn) (*apigen.HealthCheckResponse, error) {
	return nil, nil
}

func (m *mockCopyClient) GetRepositoryWithResponse(ctx context.Context, repository string, reqEditors ...apigen.RequestEditorFn) (*apigen.GetRepositoryResponse, error) {
	return nil, nil
}

func (m *mockCopyClient) StatObjectWithResponse(ctx context.Context, repository string, ref string, params *apigen.StatObjectParams, reqEditors ...apigen.RequestEditorFn) (*apigen.StatObjectResponse, error) {
	return nil, nil
}

func (m *mockCopyClient) GetObjectWithResponse(ctx context.Context, repository string, ref string, params *apigen.GetObjectParams, reqEditors ...apigen.RequestEditorFn) (*apigen.GetObjectResponse, error) {
	return nil, nil
}

func (m *mockCopyClient) UploadObjectWithBodyWithResponse(ctx context.Context, repository string, branch string, params *apigen.UploadObjectParams, contentType string, body io.Reader, reqEditors ...apigen.RequestEditorFn) (*apigen.UploadObjectResponse, error) {
	return nil, nil
}
