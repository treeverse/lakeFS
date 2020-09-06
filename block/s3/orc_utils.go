package s3

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	gproto "github.com/golang/protobuf/proto" //nolint:staticcheck // orc lib uses old proto
	"github.com/scritchley/orc/proto"
	"github.com/treeverse/lakefs/logging"
	"modernc.org/mathutil"
)

const (
	maxPostScriptSize  = 256
	orcInitialReadSize = 500
)

func GetTailLength(filename string, size int64) (int, error) {
	f, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = f.Close()
	}()
	psPlusByte := int64(maxPostScriptSize + 1)
	if psPlusByte > size {
		psPlusByte = size
	}
	// Read the last 256 bytes into buffer to get postscript
	postScriptBytes := make([]byte, psPlusByte)
	sr := io.NewSectionReader(f, size-psPlusByte, psPlusByte)
	_, err = io.ReadFull(sr, postScriptBytes)
	if err != nil {
		return 0, err
	}
	psLen := int(postScriptBytes[len(postScriptBytes)-1])
	psOffset := len(postScriptBytes) - 1 - psLen
	postScript := &proto.PostScript{}
	err = gproto.Unmarshal(postScriptBytes[psOffset:psOffset+psLen], postScript)
	if err != nil {
		return 0, err
	}
	footerLength := int(postScript.GetFooterLength())
	metadataLength := int(postScript.GetMetadataLength())
	return footerLength + metadataLength + psLen + 1, nil
}

func downloadRange(ctx context.Context, svc s3iface.S3API, logger logging.Logger, bucket string, key string, fromByte int64) (string, error) {
	f, err := ioutil.TempFile("", path.Base(key))
	if err != nil {
		return "", err
	}
	defer func() {
		_ = f.Close() // TODO log all ignored errors
	}()
	logger.Debugf("start downloading %s to local file %s", key, f.Name())
	downloader := s3manager.NewDownloaderWithClient(svc)
	var rng *string
	if fromByte > 0 {
		rng = aws.String(fmt.Sprintf("bytes=%d-", fromByte))
	}
	_, err = downloader.DownloadWithContext(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  rng,
	})
	if err != nil {
		return "", err
	}
	logger.Debugf("finished downloading %s to local file %s", key, f.Name())
	return f.Name(), nil
}

func DownloadOrcFile(ctx context.Context, svc s3iface.S3API, logger logging.Logger, bucket string, key string, footerOnly bool) (string, error) {
	var size int64
	if footerOnly {
		headObject, err := svc.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return "", err
		}
		size = *headObject.ContentLength
	}
	filename, err := downloadRange(ctx, svc, logger, bucket, key, size-orcInitialReadSize)
	if err != nil {
		return "", err
	}
	if footerOnly {
		tailLength, err := GetTailLength(filename, mathutil.MinInt64(orcInitialReadSize, size))
		if err != nil {
			return "", err
		}
		if tailLength > orcInitialReadSize {
			_ = os.Remove(filename)
			filename, err = downloadRange(ctx, svc, logger, bucket, key, size-int64(tailLength))
			if err != nil {
				return "", err
			}
		}
	}
	return filename, nil
}
