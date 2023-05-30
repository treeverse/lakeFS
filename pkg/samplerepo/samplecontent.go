package samplerepo

import (
	"bufio"
	"context"
	"io/fs"
	"os"
	"path"
	"strings"
	"text/template"
	"time"

	"github.com/go-openapi/swag"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/block"
	"github.com/treeverse/lakefs/pkg/catalog"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/samplerepo/assets"
	"github.com/treeverse/lakefs/pkg/upload"
)

const (
	sampleRepoFSRootPath = "sample"
	sampleRepoCommitMsg  = "Add sample data"
)

func PopulateSampleRepo(ctx context.Context, repo *catalog.Repository, cat catalog.Interface, pathProvider upload.PathProvider, blockAdapter block.Adapter, user *model.User) error {
	// upload sample data
	// we skip checking if the repo and branch exist, since we just created them
	// we also skip checking if the file exists, since we know the repo is empty
	readmeTmpl := path.Join(sampleRepoFSRootPath, "README.md.tmpl")
	config := map[string]string{
		"RepoName": repo.Name,
	}
	tmpl, err := template.ParseFS(assets.SampleData, "sample/README.md.tmpl")
	if err != nil {
		return err
	}

	err = fs.WalkDir(assets.SampleData, sampleRepoFSRootPath, func(p string, d fs.DirEntry, topLevelErr error) error {
		// handle a top-level error
		if topLevelErr != nil {
			return topLevelErr
		}

		if d.IsDir() {
			// noop for directories
			return nil
		}

		var (
			file fs.File
			err  error
		)
		if p == readmeTmpl {
			p = "README.md"
			f, err := os.Create(p)
			if err != nil {
				return err
			}
			defer f.Close()
			writer := bufio.NewWriter(f)
			err = tmpl.Execute(writer, config)
			if err != nil {
				return err
			}
			_, err = f.Seek(0, 0)
			if err != nil {
				return err
			}
			file = f
		} else {
			// open file from embedded FS
			file, err = assets.SampleData.Open(p)
			if err != nil {
				return err
			}
			// since we're not writing to the file, not a big risk in disregarding the error possibly returned by Close
			defer file.Close()
		}

		// get file stats for size
		fileInfo, err := file.Stat()
		if err != nil {
			return err
		}

		// write file to storage
		address := pathProvider.NewPath()
		blob, err := upload.WriteBlob(ctx, blockAdapter, repo.StorageNamespace, address, file, fileInfo.Size(), block.PutOpts{})
		if err != nil {
			return err
		}

		// create metadata entry
		writeTime := time.Now()
		entry := catalog.NewDBEntryBuilder().
			Path(strings.TrimPrefix(p, sampleRepoFSRootPath+"/")).
			PhysicalAddress(blob.PhysicalAddress).
			CreationDate(writeTime).
			Size(blob.Size).
			Checksum(blob.Checksum).
			AddressType(catalog.AddressTypeRelative).
			Build()

		// write metadata entry
		err = cat.CreateEntry(ctx, repo.Name, repo.DefaultBranch, entry)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}

	// if we succeeded, commit the changes
	// commit changes
	_, err = cat.Commit(ctx, repo.Name, repo.DefaultBranch, sampleRepoCommitMsg,
		user.Username, map[string]string{}, swag.Int64(time.Now().Unix()), nil)

	return err
}

func SampleRepoAddBranchProtection(ctx context.Context, repo *catalog.Repository, cat catalog.Interface) error {
	// Set branch protection on main branch

	err := cat.CreateBranchProtectionRule(ctx, repo.Name, repo.DefaultBranch, []graveler.BranchProtectionBlockedAction{graveler.BranchProtectionBlockedAction_COMMIT})

	return err
}
