package retention

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3control"
	"github.com/aws/aws-sdk-go/service/s3control/s3controliface"
	"github.com/treeverse/lakefs/block"
	s3Block "github.com/treeverse/lakefs/block/s3"
	"github.com/treeverse/lakefs/catalog"
	"github.com/treeverse/lakefs/fileutil"
	"github.com/treeverse/lakefs/logging"
)

// WriteExpiryResultsToSeekableReader returns a file-backed (Seeker) Reader holding the contents of expiryRows.
func WriteExpiryResultsToSeekableReader(ctx context.Context, expiryRows catalog.ExpiryRows) (fileutil.RewindableReader, error) {
	logger := logging.FromContext(ctx)

	writer, err := fileutil.NewFileWriterThenReader("expired_entries_*.json")
	if err != nil {
		return nil, fmt.Errorf("creating temporary storage to write expiry records: %w", err)
	}
	logger = logger.WithField("filename", writer.Name())
	encoder := json.NewEncoder(writer)
	count := 0
	for ; expiryRows.Next(); count++ {
		expiry, err := expiryRows.Read()
		if err != nil {
			logger.WithField("record_number", count).WithError(err).Warning("failed to read record; keep going, lose this expiry")
		}
		err = encoder.Encode(expiry)
		if err != nil {
			logger.WithFields(logging.Fields{"record_number": count, "record": expiry}).WithError(err).Warning("failed to write record; keep going, lose this expiry")
		}
	}
	reader, length, err := writer.StartReading()
	if err != nil {
		return nil, fmt.Errorf("seeking to start of file holding expiry records: %w", err)
	}
	logger.WithFields(logging.Fields{"length": length, "num_records": count}).Info("wrote expiry records")
	return reader, nil
}

type EncoderData struct {
	Writer  fileutil.WriterThenReader
	Encoder *json.Encoder
}

// BucketWriters maps buckets to WriterThenReaders handling them.
type BucketEncoders map[string]EncoderData

func (bw *BucketEncoders) GetEncoder(bucketName string) (*json.Encoder, error) {
	if quickRet, ok := (*bw)[bucketName]; ok {
		return quickRet.Encoder, nil
	}
	ret, err := fileutil.NewFileWriterThenReader(fmt.Sprintf("expiry-for-%s.csv", strings.ReplaceAll(bucketName, "/", "_")))
	if err != nil {
		return nil, err
	}
	record := EncoderData{
		Writer:  ret,
		Encoder: json.NewEncoder(ret),
	}
	(*bw)[bucketName] = record
	return record.Encoder, nil
}

// WriteExpiryManifestFromSeekableReader reads from r ExpiryResults and returns Readers to CSV
// files suitable for passing to AWS S3 batch tagging.
func WriteExpiryManifestsFromReader(ctx context.Context, c catalog.Cataloger, r io.Reader) (map[string]io.Reader, error) {
	logger := logging.FromContext(ctx)
	decoder := json.NewDecoder(r)
	bucketEncoders := BucketEncoders{}
	count := 0
	var err error
	for ; ; count++ {
		record := catalog.ExpireResult{}
		err = decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		recordLogger := logger.WithField("record_number", count)
		if err != nil {
			recordLogger.WithError(err).Warning("failed to read record; keep going, lose this expiry")
			continue
		}
		recordLogger = recordLogger.WithField("record", record)
		repository, err := c.GetRepository(ctx, record.Repository)
		if err != nil {
			recordLogger.WithError(err).Warning("failed to get repository URI; keep going, lose this expiry")
			continue
		}
		recordLogger = recordLogger.WithField("repository", repository)
		if !block.IsResolvableKey(record.PhysicalAddress) {
			recordLogger.Warning("expiry requested for nonresolvable key %s; ignore it (possible misconfiguration)", record.PhysicalAddress)
			continue
		}
		qualifiedKey, err := block.ResolveNamespace(repository.StorageNamespace, record.PhysicalAddress)
		if err != nil {
			recordLogger.WithError(err).
				Warning("could not resolve namespace; keep going, lose this expiry")
		}
		recordLogger = recordLogger.WithField("qualified_key", qualifiedKey)
		if qualifiedKey.StorageType != block.StorageTypeS3 {
			recordLogger.
				Warning("cannot expire on repository which is not on S3; keep going, lose this expiry")
			continue
		}
		bucketName := qualifiedKey.StorageNamespace
		encoder, err := bucketEncoders.GetEncoder(bucketName)
		if err != nil {
			recordLogger.WithError(err).
				Warning("failed to prepare encoder; keep going, lose this expiry")
		}
		err = encoder.Encode([]string{bucketName, qualifiedKey.Key})
		if err != nil {
			recordLogger.WithError(err).
				Warningf("failed to encode CSV row for %s,%s: %s; keep going, lose this expiry", bucketName, qualifiedKey.Key, err)
			continue
		}
	}
	if !errors.Is(err, io.EOF) {
		return nil, err
	}
	ret := map[string]io.Reader{}
	for bucket, encodingData := range bucketEncoders {
		resetableReader, count, err := encodingData.Writer.StartReading()
		bucketLogger := logger.WithField("bucket", bucket)
		if err != nil {
			bucketLogger.WithError(err).Error("failed to start reading encoded CSVs; lose all bucket expiries")
			continue
		}
		bucketLogger.WithField("bytes", count).Info("wrote encoded CSV for bucket expiry")
		if count > 0 {
			ret[bucket] = resetableReader
		}
	}
	return ret, nil
}

// BatchTagOnS3BucketParams holds tagging configuration for BatchTagOnS3Bucket.
type BatchTagOnS3BucketParams struct {
	// Account to perform tagging (required for S3 API)
	AccountId string
	// Role for performing tagging
	RoleArn string

	// Name of bucket holding objects to tag
	BucketName string

	// Path to use for manifest on S3, in "S3BatchOperations_CSV_20180820" format
	ManifestUrl string
}

// BatchTagOnS3Bucket uses client (which should be on the right region) to start an AWS S3 batch
// tagging operation in bucket according to the CSV contents of reader.
func BatchTagOnS3Bucket(ctx context.Context, s3ControlClient s3controliface.S3ControlAPI, s3Client s3iface.S3API, reader io.ReadSeeker, params BatchTagOnS3BucketParams) error {
	manifestUrl, err := url.Parse(params.ManifestUrl)
	if err != nil {
		return fmt.Errorf("parse manifest URL %s: %w", params.ManifestUrl, err)
	}
	if manifestUrl.Scheme != "s3" {
		return fmt.Errorf("manifest URL %s not on S3", params.ManifestUrl)
	}
	// Upload to S3 and get ETag.
	manifestParams := s3.PutObjectInput{
		Body:    reader,
		Bucket:  &manifestUrl.Host,
		Key:     &manifestUrl.Path,
		Tagging: aws.String("service=lakeFS&type=retention-manifest"),
	}
	upload, err := s3Client.PutObject(&manifestParams)
	if err != nil {
		return fmt.Errorf("putObject %+v: %w", manifestParams, err)
	}
	etag := upload.ETag
	manifestArn := fmt.Sprintf("arn:aws:s3:::%s/%s", manifestUrl.Host, manifestUrl.Path)

	input := s3control.CreateJobInput{
		AccountId: &params.AccountId,
		// Use client request tokens to prevent expiring too often.  This is only an
		// emergency brake.
		ClientRequestToken: aws.String(fmt.Sprintf("expire-%s-at-%s", manifestUrl.Host, time.Now().Round(5*time.Minute))),
		Description:        aws.String("automated tag to expire objects"),
		Manifest: &s3control.JobManifest{
			Location: &s3control.JobManifestLocation{
				ETag:      etag,
				ObjectArn: aws.String(manifestArn),
			},
			Spec: &s3control.JobManifestSpec{
				Fields: []*string{aws.String("Bucket"), aws.String("Key")},
				Format: aws.String(s3control.JobManifestFormatS3batchOperationsCsv20180820),
			},
		},
		Operation: &s3control.JobOperation{
			S3PutObjectTagging: &s3control.S3SetObjectTaggingOperation{
				TagSet: []*s3control.S3Tag{{Key: aws.String(s3Block.ExpireObjectS3Tag), Value: aws.String("1")}},
			},
		},
		// TODO(ariels): allow configuration
		Priority: aws.Int64(10),
		// TODO(ariels): allow configuration of Report field

		RoleArn: &params.RoleArn,
		Tags:    []*s3control.S3Tag{{Key: aws.String("service"), Value: aws.String("lakeFS")}},
	}
	result, err := s3ControlClient.CreateJob(&input)
	if err != nil {
		return fmt.Errorf("create tagging job %+v: %w", input, err)
	}
	logging.FromContext(ctx).WithFields(logging.Fields{
		"manifest": params.ManifestUrl,
		"bucket":   params.BucketName,
		"job_id":   *result.JobId,
	}).Info("started S3 batch tagging job")
	return nil
}
