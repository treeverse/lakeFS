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

// getTailLength reads the ORC postscript from the given file, returning the full tail length.
// The tail length equals (footer + metadata + postscript + 1) bytes.
func getTailLength(filename string, size int64, logger logging.Logger) (int, error) {
	f, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err := f.Close(); err != nil {
			logger.Errorf("failed to close orc file after reading tail. file=%s, err=%w", f.Name(), err)
		}
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
		if err := f.Close(); err != nil {
			logger.Errorf("failed to close orc file after download. file=%s, err=%w", f.Name(), err)
		}
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

// DownloadOrc downloads from s3 to a temporary file, returning the new file location.
// If tailOnly is set to true, download only the tail (metadata+footer) by try the last `orcInitialReadSize` bytes of the file.
// Then, check the last byte to see if the whole tail was downloaded. If not, download again with the actual tail length.
func DownloadOrc(ctx context.Context, svc s3iface.S3API, logger logging.Logger, bucket string, key string, tailOnly bool) (string, error) {
	var size int64
	if tailOnly {
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
	if tailOnly {
		tailLength, err := getTailLength(filename, mathutil.MinInt64(orcInitialReadSize, size), logger)
		if err != nil {
			return "", err
		}
		if tailLength > orcInitialReadSize {
			// tail didn't fit in initially downloaded file
			if err := os.Remove(filename); err != nil {
				logger.Errorf("failed to delete orc file %s: %w", filename, err)
			}
			filename, err = downloadRange(ctx, svc, logger, bucket, key, size-int64(tailLength))
			if err != nil {
				return "", err
			}
		}
	}
	return filename, nil
}
