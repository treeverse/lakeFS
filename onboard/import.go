package onboard

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	log "github.com/sirupsen/logrus"
	"github.com/treeverse/lakefs/catalog"
	"time"
)

const (
	LauncherBranchName        = "launcher"
	LauncherCommitMsgTemplate = "Import from %s"
)

func Import(ctx context.Context, svc s3iface.S3API, cataloger catalog.Cataloger, manifestURL string, repository string) error {
	repo, err := cataloger.GetRepository(ctx, repository)
	if err != nil {
		return err
	}

	err = cataloger.CreateBranch(ctx, repository, LauncherBranchName, repo.DefaultBranch)
	hasPreviousCommit := false
	if err != nil {
		commits, _, err := cataloger.ListCommits(ctx, repository, LauncherBranchName, LauncherBranchName+":HEAD", 1)
		if err == nil && len(commits) > 0 {
			hasPreviousCommit = true
		}
	}
	manifest, err := FetchManifest(svc, manifestURL)
	if err != nil {
		return err
	}
	rowsChan, err := FetchInventory(ctx, svc, *manifest)
	if err != nil {
		return err
	}

	for row := range rowsChan {
		if row.Error != nil {
			log.Errorf("failed to read row from inventory: %v", row.Error)
			continue
		}
		if !row.IsLatest || row.IsDeleteMarker {
			continue
		}
		if hasPreviousCommit {
			//prevEntry, err := cataloger.GetEntry(ctx, repository, LauncherBranchName+":HEAD", row.Key)
			//if err != nil && !errors.Is(err, db.ErrNotFound) {
			//	log.Errorf("failed to get previous state of entry: %v", err)
			//} else if prevEntry.Checksum == row.ETag {
			//	log.Info("Entry already done, skipping")
			//	continue
			//}
		}
		entry := catalog.Entry{
			Path:            row.Key,
			PhysicalAddress: "s3://" + row.Bucket + "/" + row.Key,
			CreationDate:    time.Unix(0, row.LastModified*int64(time.Millisecond)),
			Size:            *row.Size,
			Checksum:        row.ETag,
			Metadata:        nil,
		}
		err = cataloger.CreateEntry(ctx, repository, LauncherBranchName, entry)
		if err != nil {
			log.Errorf("failed to create entry %s (%v)", row.Key, err)
		}
	}

	_, err = cataloger.Commit(ctx, repository, LauncherBranchName, fmt.Sprintf(LauncherCommitMsgTemplate, manifest.SourceBucket), "lakeFS", catalog.Metadata{"manifest_url": manifestURL, "source_bucket": manifest.SourceBucket})
	return err
}
