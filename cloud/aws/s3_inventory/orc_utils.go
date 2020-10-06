package s3_inventory

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
	"github.com/go-openapi/swag"
	gproto "github.com/golang/protobuf/proto" //nolint:staticcheck // orc lib uses old proto
	"github.com/scritchley/orc/proto"
	"github.com/treeverse/lakefs/logging"
)

const (
	maxPostScriptSize  = 256 // from the ORC specification: https://orc.apache.org/specification/ORCv1/
	orcInitialReadSize = 16000
)

// getTailLength reads the ORC postscript from the given file, returning the full tail length.
// The tail length equals (footer + metadata + postscript + 1) bytes.
func getTailLength(f *os.File) (int, error) {
	stat, err := f.Stat()
	if err != nil {
		return 0, err
	}
	fileSize := stat.Size()
	psPlusByte := int64(maxPostScriptSize + 1)
	if psPlusByte > fileSize {
		psPlusByte = fileSize
	}
	// Read the last 256 bytes into buffer to get postscript
	postScriptBytes := make([]byte, psPlusByte)
	sr := io.NewSectionReader(f, fileSize-psPlusByte, psPlusByte)
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

func downloadRange(ctx context.Context, svc s3iface.S3API, logger logging.Logger, bucket string, key string, fromByte int64) (*os.File, error) {
	f, err := ioutil.TempFile("", path.Base(key))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := os.Remove(f.Name()); err != nil {
			logger.Errorf("failed to remove orc file after download. file=%s, err=%w", f.Name(), err)
		}
	}()
	downloader := s3manager.NewDownloaderWithClient(svc)
	var rng *string
	if fromByte > 0 {
		rng = aws.String(fmt.Sprintf("bytes=%d-", fromByte))
	}
	logger.Debugf("start downloading %s[%s] to local file %s", key, swag.StringValue(rng), f.Name())
	_, err = downloader.DownloadWithContext(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  rng,
	})
	if err != nil {
		return nil, err
	}
	logger.Debugf("finished downloading %s to local file %s", key, f.Name())
	return f, nil
}

// DownloadOrc downloads a file from s3 and returns a ReaderSeeker to it.
// If tailOnly is set to true, download only the tail (metadata+footer) by trying the last `orcInitialReadSize` bytes of the file.
// Then, check the last byte to see if the whole tail was downloaded. If not, download again with the actual tail length.
func DownloadOrc(ctx context.Context, svc s3iface.S3API, logger logging.Logger, bucket string, key string, tailOnly bool) (*OrcFile, error) {
	var size int64
	if tailOnly {
		headObject, err := svc.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, err
		}
		size = *headObject.ContentLength
	}
	f, err := downloadRange(ctx, svc, logger, bucket, key, size-orcInitialReadSize)
	if err != nil {
		return nil, err
	}
	if tailOnly {
		tailLength, err := getTailLength(f)
		if err != nil {
			return nil, err
		}
		if tailLength > orcInitialReadSize {
			// tail didn't fit in initially downloaded file
			if err = f.Close(); err != nil {
				logger.Errorf("failed to close orc file. file=%s, err=%w", f.Name(), err)
			}
			f, err = downloadRange(ctx, svc, logger, bucket, key, size-int64(tailLength))
			if err != nil {
				return nil, err
			}
		}
	}
	return &OrcFile{f}, nil
}

type OrcFile struct {
	*os.File
}

func (or *OrcFile) Size() int64 {
	stats, err := or.Stat()
	if err != nil {
		return 0
	}
	return stats.Size()
}

func (or *OrcFile) Close() error {
	return or.File.Close()
}
