package local

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/gabriel-vasile/mimetype"
	"github.com/go-openapi/swag"

	"github.com/treeverse/lakefs/pkg/api"
	"github.com/treeverse/lakefs/pkg/uri"
)

type APIWrapper struct {
	ctx    context.Context
	client api.ClientWithResponsesInterface
}

func NewAPIWrapper(ctx context.Context, client api.ClientWithResponsesInterface) *APIWrapper {
	return &APIWrapper{
		ctx:    ctx,
		client: client,
	}
}

func (w *APIWrapper) Dereference(refUri *uri.URI) (string, error) {
	branchResponse, err := w.client.GetBranchWithResponse(w.ctx, refUri.Repository, refUri.Ref)
	if err != nil {
		return "", err
	}
	stableRef := refUri.Ref
	if branchResponse.StatusCode() == http.StatusNotFound {
		stableRef = refUri.Ref
	} else if branchResponse.StatusCode() == http.StatusOK {
		stableRef = branchResponse.JSON200.CommitId
	} else {
		return "", fmt.Errorf("could not read branch status: HTTP %d", branchResponse.StatusCode())
	}
	return stableRef, nil
}

func (w *APIWrapper) HasUncommittedChanges(branchUri *uri.URI) (bool, error) {
	diffResponse, err := w.client.DiffBranchWithResponse(w.ctx, branchUri.Repository, branchUri.Ref, &api.DiffBranchParams{
		Amount: api.PaginationAmountPtr(1),
	})
	if err != nil {
		return false, err
	}
	if diffResponse.StatusCode() == http.StatusNotFound {
		return false, fmt.Errorf("could not find branch: '%s'", branchUri.Ref)
	}
	if diffResponse.StatusCode() != http.StatusOK {
		return false, fmt.Errorf("could not lookup uncommitted changes for branch: '%s'", branchUri.Ref)
	}
	return len(diffResponse.JSON200.Results) > 0, nil
}

func (w *APIWrapper) Commit(branchUri *uri.URI, message string, kvPairs map[string]string) (string, error) {
	metadata := api.CommitCreation_Metadata{
		AdditionalProperties: kvPairs,
	}
	resp, err := w.client.CommitWithResponse(w.ctx, branchUri.Repository, branchUri.Ref, &api.CommitParams{}, api.CommitJSONRequestBody{
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

func (w *APIWrapper) DeleteObject(target *uri.URI) error {
	response, err := w.client.DeleteObjectWithResponse(w.ctx, target.Repository, target.Ref, &api.DeleteObjectParams{
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

func (w *APIWrapper) PathExists(dest *uri.URI) (bool, error) {
	pth := dest.GetPath()
	if !strings.HasSuffix(pth, "/") {
		pth = pth + "/"
	}

	response, err := w.client.ListObjectsWithResponse(w.ctx, dest.Repository, dest.Ref, &api.ListObjectsParams{
		After:     nil,
		Amount:    api.PaginationAmountPtr(1),
		Delimiter: (*api.PaginationDelimiter)(swag.String("/")),
		Prefix:    (*api.PaginationPrefix)(dest.Path),
	})
	if err != nil {
		return false, err
	}
	if response.StatusCode() != http.StatusOK {
		return false, fmt.Errorf("could not check path '%s': HTTP %d", dest.GetPath(), response.StatusCode())
	}
	return response.JSON200.Pagination.Results > 0, nil
}

func (w *APIWrapper) WriteObject(localFile string, dest *uri.URI, bar *pb.ProgressBar) error {
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
		bar.SetTotal(info.Size())
		bar.SetCurrent(0)
		reader = bar.NewProxyReader(f)
		defer func() {
			_ = f.Close()
		}()
	}

	phyResponse, err := w.client.GetPhysicalAddressWithResponse(w.ctx, dest.Repository, dest.Ref, &api.GetPhysicalAddressParams{
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

	request, err := http.NewRequestWithContext(w.ctx, http.MethodPut, *uploadUrl, reader)
	if err != nil {
		return err
	}
	request.ContentLength = info.Size()

	uploadResponse, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}

	bar.Finish()
	if uploadResponse.StatusCode > 299 {
		return fmt.Errorf("failed to upload file %s: HTTP %d", localFile, uploadResponse.StatusCode)
	}
	response, err := w.client.LinkPhysicalAddressWithResponse(w.ctx, dest.Repository, dest.Ref, &api.LinkPhysicalAddressParams{
		Path: dest.GetPath(),
	}, api.LinkPhysicalAddressJSONRequestBody{
		Checksum:    uploadResponse.Header.Get("ETag"),
		ContentType: swag.String(contentType),
		SizeBytes:   info.Size(),
		Mtime:       swag.Int64(info.ModTime().Unix()),
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

func (w *APIWrapper) ListDir(source *uri.URI) ([]api.ObjectStats, error) {
	hasMore := true
	after := ""
	prefix := source.GetPath()

	files := make([]api.ObjectStats, 0)

	for hasMore {
		listing, err := w.client.ListObjectsWithResponse(w.ctx, source.Repository, source.Ref, &api.ListObjectsParams{
			Presign: swag.Bool(true), // TODO(ozk): can we still support Azure? local?
			After:   (*api.PaginationAfter)(swag.String(after)),
			Prefix:  (*api.PaginationPrefix)(swag.String(prefix)),
		})
		if err != nil {
			return nil, err
		}
		if listing.StatusCode() != http.StatusOK {
			return nil, fmt.Errorf("could not list remote ref '%s': HTTP %d", source.Ref, listing.StatusCode())
		}

		for _, object := range listing.JSON200.Results {
			files = append(files, object)
		}

		hasMore = listing.JSON200.Pagination.HasMore
		after = listing.JSON200.Pagination.NextOffset
	}
	return files, nil
}

func (w *APIWrapper) UploadDirectoryChanges(dest *uri.URI, localDirectory string, maxParallelism int) error {
	// find changes
	diff, err := DiffPath(localDirectory)
	if err != nil {
		return err
	}
	if diff.IsClean() {
		return nil
	}

	totalActions := len(diff.Modified) + len(diff.Removed) + len(diff.Added)
	parallelDownloads := maxParallelism
	if totalActions < parallelDownloads {
		parallelDownloads = totalActions
	}

	bars := make([]*pb.ProgressBar, parallelDownloads)
	for i := range bars {
		bars[i] = ProgressBar.New(0)
	}
	pool := pb.NewPool()
	mainBar := ProgressBar.New(totalActions)
	mainBar.Set("prefix", "applying diff")
	pool.Add(mainBar)
	pool.Add(bars...)
	if err := pool.Start(); err != nil {
		return err
	}

	ch := make(chan *pb.ProgressBar, parallelDownloads)
	for _, bar := range bars {
		ch <- bar
	}
	errCh := make(chan error, 0)
	doneCh := make(chan bool, 0)
	var wg sync.WaitGroup
	wg.Add(totalActions)

	// upload them
	for _, d := range diff.Added {
		go func(d string) {
			bar := <-ch
			filePath := path.Join(localDirectory, d)
			bar.Set("prefix", fmt.Sprintf("Adding %s", d))
			err = w.WriteObject(filePath, &uri.URI{
				Repository: dest.Repository,
				Ref:        dest.Ref,
				Path:       swag.String(path.Join(dest.GetPath(), d)),
			}, bar)
			if err != nil {
				errCh <- err
			} else {
				mainBar.Increment()
				wg.Done()
				ch <- bar // release
			}
		}(d)
	}
	for _, d := range diff.Modified {
		go func(d string) {
			bar := <-ch
			filePath := path.Join(localDirectory, d)
			bar.Set("prefix", fmt.Sprintf("Replacing %s", d))
			err = w.WriteObject(filePath, &uri.URI{
				Repository: dest.Repository,
				Ref:        dest.Ref,
				Path:       swag.String(path.Join(dest.GetPath(), d)),
			}, bar)
			if err != nil {
				errCh <- err
			} else {
				mainBar.Increment()
				wg.Done()
				ch <- bar // release
			}
		}(d)
	}
	for _, d := range diff.Removed {
		go func(d string) {
			bar := <-ch
			bar.Set("prefix", fmt.Sprintf("Deleting %s", path.Join(dest.GetPath(), d)))
			bar.SetTotal(100)
			bar.SetCurrent(0)
			err = w.DeleteObject(&uri.URI{
				Repository: dest.Repository,
				Ref:        dest.Ref,
				Path:       swag.String(path.Join(dest.GetPath(), d)),
			})
			if err != nil {
				errCh <- err
			} else {
				bar.SetCurrent(100)
				mainBar.Increment()
				wg.Done()
				ch <- bar // release
			}
		}(d)
	}

	go func() {
		wg.Wait() // wait until all downloads are done
		doneCh <- true
	}()

	select {
	case <-doneCh:
		break
	case err := <-errCh:
		return err
	}

	return pool.Stop()
}

func (w *APIWrapper) SyncDirectory(source *uri.URI, localDirectory string, maxParallelism int) error {
	indexer := &ObjectTracker{}

	fileSet := make(map[string]bool)

	// we're syncing a directory, let's make sure we normalize it by adding a '/' at the end:
	if !strings.HasSuffix(source.GetPath(), PathSeparator) {
		source.Path = swag.String(source.GetPath() + PathSeparator)
	}
	// list the things
	filesWeNeed, err := w.ListDir(source)
	if err != nil {
		return err
	}

	parallelDownloads := maxParallelism
	if len(filesWeNeed) < parallelDownloads {
		parallelDownloads = len(filesWeNeed)
	}

	bars := make([]*pb.ProgressBar, parallelDownloads)
	for i := range bars {
		bars[i] = ProgressBar.New(0)
	}
	pool := pb.NewPool()
	mainBar := ProgressBar.New(len(filesWeNeed))
	mainBar.Set("prefix", fmt.Sprintf("objects in '%s'", source.String()))
	pool.Add(mainBar)
	pool.Add(bars...)
	if err := pool.Start(); err != nil {
		return err
	}

	ch := make(chan *pb.ProgressBar, parallelDownloads)
	for _, bar := range bars {
		ch <- bar
	}
	errCh := make(chan error, 0)
	doneCh := make(chan bool, 0)
	var wg sync.WaitGroup
	wg.Add(len(filesWeNeed))

	fileSetLock := sync.RWMutex{}
	skippedObjects := 0

	// ensure data directory exists
	err = CreateDirectoryTree(localDirectory)
	if err != nil {
		return err
	}
	fileSystem := os.DirFS(localDirectory)
	// current index:
	index, err := ReadIndex(localDirectory)
	if err != nil {
		return err
	}

	// download the things
	for _, obj := range filesWeNeed {
		bar := <-ch // block until we have a slot
		go func(obj api.ObjectStats) {
			currentPath := strings.TrimPrefix(obj.Path, source.GetPath())
			var entry *Object
			if index != nil {
				// read this path from index
				entry = index.Find(currentPath)
			}
			skipped, err := DownloadRemoteObject(obj, source.GetPath(), localDirectory, bar, indexer, entry)
			if err != nil {
				errCh <- err
				return
			}
			// update fileset
			fileSetLock.Lock()
			if skipped {
				skippedObjects += 1
			}
			fileSet[currentPath] = true // add to download set
			// recursively also add all parents
			d := path.Dir(currentPath)
			for d != "" && d != "." && d != "/" {
				fileSet[d] = true
				d = path.Dir(d)
			}
			fileSetLock.Unlock()
			mainBar.Increment()
			wg.Done()
			ch <- bar // release
		}(obj)
	}
	go func() {
		wg.Wait() // wait until all downloads are done
		doneCh <- true
	}()

	select {
	case <-doneCh:
		break
	case err := <-errCh:
		return err
	}

	if err := pool.Stop(); err != nil {
		return err
	}

	fmt.Printf("Finishing syncing directory '%s' (%d/%d skipped)\n\n", localDirectory, skippedObjects, len(filesWeNeed))

	// iterate over local files now
	skipPrefix := ""
	err = fs.WalkDir(fileSystem, ".", func(p string, d fs.DirEntry, err error) error {
		if p == "." || p == IndexFileName {
			return nil
		}
		if skipPrefix != "" && HasParentDir(p, skipPrefix) {
			// we can skip as we removed a parent folder
			return nil
		}
		fullPath := path.Join(localDirectory, p)
		_, written := fileSet[p]
		if !written && d.IsDir() {
			err = RemoveDirectoryTree(fullPath)
			if err != nil {
				return err
			}
		} else if !written {
			err = RemoveFile(fullPath)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// write the index
	return WriteIndex(
		localDirectory,
		&Index{
			Objects: indexer.GetObjects(),
		},
	)
}
