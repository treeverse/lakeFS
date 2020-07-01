package onboard

import (
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"net/url"
)

type Manifest struct {
	URL                string         `json:"-"`
	InventoryBucketArn string         `json:"destinationBucket"`
	SourceBucket       string         `json:"sourceBucket"`
	Files              []ManifestFile `json:"files"`
	Format             string         `json:"fileFormat"`
}

type ManifestFile struct {
	Key         string `json:"key"`
	Size        int    `json:"size"`
	MD5checksum string `json:"MD5checksum"`
}

func LoadManifest(manifestURL string, s3svc s3iface.S3API) (manifest *Manifest, err error) {
	u, err := url.Parse(manifestURL)
	if err != nil {
		return
	}
	output, err := s3svc.GetObject(&s3.GetObjectInput{Bucket: &u.Host, Key: &u.Path})
	if err != nil {
		return
	}
	manifest = new(Manifest)
	err = json.NewDecoder(output.Body).Decode(manifest)
	if err != nil {
		return
	}
	if manifest.Format != "Parquet" {
		return nil, errors.New("currently only parquet inventories are supported. got: " + manifest.Format)
	}
	manifest.URL = manifestURL
	return
}
