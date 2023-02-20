package local

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"

	"github.com/gabriel-vasile/mimetype"
	"github.com/go-openapi/swag"

	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

func DereferenceBranch(ctx context.Context, client api.ClientWithResponsesInterface, u *uri.URI) (string, error) {
	branchResponse, err := client.GetBranchWithResponse(ctx, u.Repository, u.Ref)
	if err != nil {
		return "", err
	}
	stableRef := u.Ref
	if branchResponse.StatusCode() == http.StatusNotFound {
		stableRef = u.Ref
	} else if branchResponse.StatusCode() == http.StatusOK {
		stableRef = branchResponse.JSON200.CommitId
	} else {
		return "", fmt.Errorf("could not read branch status: HTTP %d", branchResponse.StatusCode())
	}
	return stableRef, nil
}

func HasUncommittedChanges(ctx context.Context, client api.ClientWithResponsesInterface, u *uri.URI) (bool, error) {
	diffResponse, err := client.DiffBranchWithResponse(ctx, u.Repository, u.Ref, &api.DiffBranchParams{
		Amount: api.PaginationAmountPtr(1),
	})
	if err != nil {
		return false, err
	}
	if diffResponse.StatusCode() == http.StatusNotFound {
		return false, fmt.Errorf("could not find branch: '%s'", u.Ref)
	}
	if diffResponse.StatusCode() != http.StatusOK {
		return false, fmt.Errorf("could not lookup uncommitted changes for branch: '%s'", u.Ref)
	}
	return len(diffResponse.JSON200.Results) > 0, nil
}

func Commit(ctx context.Context, client api.ClientWithResponsesInterface, u *uri.URI, message string, kvPairs map[string]string) (string, error) {
	metadata := api.CommitCreation_Metadata{
		AdditionalProperties: kvPairs,
	}
	resp, err := client.CommitWithResponse(ctx, u.Repository, u.Ref, &api.CommitParams{}, api.CommitJSONRequestBody{
		Message:  message,
		Metadata: &metadata,
	})
	if err != nil {
		return "", err
	}
	switch resp.StatusCode() {
	case http.StatusCreated:
		return resp.JSON201.Id, nil
	case http.StatusBadRequest:
		return "", fmt.Errorf(resp.JSON400.Message)
	case http.StatusUnauthorized:
		return "", fmt.Errorf(resp.JSON401.Message)
	case http.StatusNotFound:
		return "", fmt.Errorf(resp.JSON404.Message)
	case http.StatusPreconditionFailed:
		return "", fmt.Errorf(resp.JSON412.Message)
	default:
		return "", fmt.Errorf(resp.JSONDefault.Message)
	}
}

func GetCurrentUserId(ctx context.Context, client api.ClientWithResponsesInterface) (string, error) {
	response, err := client.GetCurrentUserWithResponse(ctx)
	if err != nil {
		return "", err
	}
	if err == nil && response.StatusCode() == http.StatusOK {
		return response.JSON200.User.Id, nil
	}
	return "", fmt.Errorf("could not retrieve current lakeFS user: HTTP %d", response.StatusCode())
}

func UploadRecursive(ctx context.Context, client api.ClientWithResponsesInterface, directoryPath string, destination *uri.URI) error {
	fileSystem := os.DirFS(directoryPath)
	return fs.WalkDir(fileSystem, ".", func(p string, d fs.DirEntry, err error) error {
		if p == "." || (d != nil && d.IsDir()) {
			return nil
		}
		fullLocalPath := path.Join(directoryPath, p)
		fmt.Printf("writing path: %s\n", fullLocalPath)
		// write to destination
		return WriteLakeFSFile(ctx, client, fullLocalPath, &uri.URI{
			Repository: destination.Repository,
			Ref:        destination.Ref,
			Path:       swag.String(path.Join(destination.GetPath(), p)),
		})
	})
}

func DeleteLakeFSFile(ctx context.Context, client api.ClientWithResponsesInterface, target *uri.URI) error {
	response, err := client.DeleteObjectWithResponse(ctx, target.Repository, target.Ref, &api.DeleteObjectParams{
		Path: target.GetPath(),
	})
	if err != nil {
		return err
	}
	if response.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("could not delete object: %s: HTTP %d", target.GetPath(), response.StatusCode())
	}
	return nil
}

func WriteLakeFSFile(ctx context.Context, client api.ClientWithResponsesInterface, localFile string, dest *uri.URI) error {
	info, err := os.Stat(localFile)
	if err != nil {
		return err
	}

	contentType := "application/octet-stream"
	var reader io.Reader
	if info.Size() > 0 {
		mime, err := mimetype.DetectFile(localFile)
		if err == nil {
			contentType = mime.String()
		}
		f, err := os.Open(localFile)
		if err != nil {
			return err
		}
		defer func() {
			_ = f.Close()
		}()
		reader = f
	}

	phyResponse, err := client.GetPhysicalAddressWithResponse(ctx, dest.Repository, dest.Ref, &api.GetPhysicalAddressParams{
		Path:    dest.GetPath(),
		Presign: swag.Bool(true),
	})
	if err != nil {
		return err
	}
	if phyResponse.StatusCode() != http.StatusOK {
		return fmt.Errorf("could not upload object %s: got HTTP %d when requesting a pre-signed url", localFile, phyResponse.StatusCode())
	}
	uploadUrl := phyResponse.JSON200.PresignedUrl

	request, err := http.NewRequestWithContext(ctx, http.MethodPut, *uploadUrl, reader)
	if err != nil {
		return err
	}
	request.ContentLength = info.Size()
	uploadResponse, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}

	if uploadResponse.StatusCode > 299 {
		return fmt.Errorf("failed to upload file %s: HTTP %d", localFile, uploadResponse.StatusCode)
	}
	response, err := client.LinkPhysicalAddressWithResponse(ctx, dest.Repository, dest.Ref, &api.LinkPhysicalAddressParams{
		Path: dest.GetPath(),
	}, api.LinkPhysicalAddressJSONRequestBody{
		Checksum:    uploadResponse.Header.Get("ETag"),
		ContentType: swag.String(contentType),
		SizeBytes:   info.Size(),
		Staging: api.StagingLocation{
			PhysicalAddress: phyResponse.JSON200.PhysicalAddress,
			PresignedUrl:    uploadUrl,
			Token:           phyResponse.JSON200.Token,
		},
	})
	if err != nil {
		return err
	}
	if response.StatusCode() != http.StatusOK {
		return fmt.Errorf("could not stage file %s on lakeFS: HTTP %d\n%s\n",
			localFile, response.StatusCode(), response.Body)
	}
	return nil
}
