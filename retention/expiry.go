package retention

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

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

type CsvWriterData struct {
	Writer    fileutil.WriterThenReader
	CsvWriter *csv.Writer
}

// BucketWriters maps buckets to WriterThenReaders handling them.
type BucketWriters map[string]CsvWriterData

func (bw *BucketWriters) GetWriter(bucketName string) (*csv.Writer, error) {
	if quickRet, ok := (*bw)[bucketName]; ok {
		return quickRet.CsvWriter, nil
	}
	ret, err := fileutil.NewFileWriterThenReader(fmt.Sprintf("expiry-for-%s.csv", strings.ReplaceAll(bucketName, "/", "_")))
	if err != nil {
		return nil, err
	}
	record := CsvWriterData{
		Writer:    ret,
		CsvWriter: csv.NewWriter(ret),
	}
	(*bw)[bucketName] = record
	return record.CsvWriter, nil
}

// WriteExpiryManifestFromSeekableReader reads from r ExpiryResults and returns Readers to CSV
// files suitable for passing to AWS S3 batch tagging.
func WriteExpiryManifestsFromReader(ctx context.Context, c catalog.Cataloger, r io.Reader) (map[string]fileutil.RewindableReader, error) {
	logger := logging.FromContext(ctx)
	decoder := json.NewDecoder(r)
	bucketWriters := BucketWriters{}

	var err error
	for recordNumber := 0; ; recordNumber++ {
		recordLogger := logger.WithField("record_number", recordNumber)
		record := catalog.ExpireResult{}
		err = decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
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
		csvWriter, err := bucketWriters.GetWriter(bucketName)
		if err != nil {
			recordLogger.WithError(err).
				Warning("failed to prepare CSV encoder; keep going, lose this expiry")
		}
		err = csvWriter.Write([]string{bucketName, qualifiedKey.Key})
		if err != nil {
			recordLogger.WithError(err).
				Warningf("failed to encode CSV row for %s,%s: %s; keep going, lose this expiry", bucketName, qualifiedKey.Key, err)
			continue
		}
	}
	if !errors.Is(err, io.EOF) {
		return nil, err
	}
	ret := map[string]fileutil.RewindableReader{}
	for bucket, encodingData := range bucketWriters {
		filename := encodingData.Writer.Name()
		bucketLogger := logger.WithFields(logging.Fields{"filename": filename, "bucket": bucket})
		encodingData.CsvWriter.Flush()
		err := encodingData.CsvWriter.Error()
		if err != nil {
			bucketLogger.WithError(err).Error("failed to flush encoded CSV; lose all bucket expiries")
			continue
		}
		resetableReader, count, err := encodingData.Writer.StartReading()
		if err != nil {
			bucketLogger.WithError(err).Error("failed to start reading encoded CSVs; lose all bucket expiries")
			continue
		}
		bucketLogger.WithField("bytes", count).Info("wrote encoded CSV for bucket expiry")
		if count >= 0 {
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
func BatchTagOnS3Bucket(ctx context.Context, s3ControlClient s3controliface.S3ControlAPI, s3Client s3iface.S3API, reader io.ReadSeeker, params *BatchTagOnS3BucketParams) error {
	manifestUrl, err := url.Parse(params.ManifestUrl)
	if err != nil {
		return fmt.Errorf("parse manifest URL %s: %w", params.ManifestUrl, err)
	}
	logger := logging.FromContext(ctx).WithFields(logging.Fields{
		"manifest": params.ManifestUrl,
		"bucket":   params.BucketName,
	})
	if manifestUrl.Scheme != "s3" {
		return fmt.Errorf("manifest URL %s not on S3", params.ManifestUrl)
	}
	trimmedPath := strings.TrimPrefix(manifestUrl.Path, "/")
	// Upload to S3 and get ETag.
	manifestParams := s3.PutObjectInput{
		Body:    reader,
		Bucket:  &manifestUrl.Host,
		Key:     &trimmedPath,
		Tagging: aws.String("service=lakeFS&type=retention-manifest"),
	}
	upload, err := s3Client.PutObject(&manifestParams)
	if err != nil {
		return fmt.Errorf("putObject %+v: %w", manifestParams, err)
	}
	// PutObject includes _quotes_ around the etag.  Strip it.
	etag := strings.TrimSuffix(strings.TrimPrefix(*upload.ETag, "\""), "\"")
	logger.WithField("etag", etag).Info("Manifest uploaded")
	manifestArn := fmt.Sprintf("arn:aws:s3:::%s/%s", manifestUrl.Host, trimmedPath)

	input := s3control.CreateJobInput{
		AccountId:            &params.AccountId,
		ConfirmationRequired: aws.Bool(false),
		// TODO(ariels): use ClientRequestToken to help avoid flooding?
		Description: aws.String("automated tag to expire objects"),
		Manifest: &s3control.JobManifest{
			Location: &s3control.JobManifestLocation{
				ETag:      aws.String(etag),
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
		Report: &s3control.JobReport{
			Enabled: aws.Bool(false),
		},
		RoleArn: &params.RoleArn,
		Tags: []*s3control.S3Tag{
			{Key: aws.String("service"), Value: aws.String("lakeFS")},
			{Key: aws.String("job"), Value: aws.String("tag-expiry")},
		},
	}
	result, err := s3ControlClient.CreateJob(&input)
	if err != nil {
		return fmt.Errorf("create tagging job %+v: %w", input, err)
	}
	logger.WithField("job_id", *result.JobId).Info("started S3 batch tagging job")
	return nil
}

// ExpireOnS3Params holds configuration for ExpireOnS3.
type ExpireOnS3Params struct {
	AccountId            string
	RoleArn              string
	ManifestUrlForBucket func(string) string
}

func ExpireOnS3(ctx context.Context, s3ControlClient s3controliface.S3ControlAPI, s3Client s3iface.S3API, c catalog.Cataloger, expiryResultsReader fileutil.RewindableReader, params *ExpireOnS3Params) {
	logger := logging.FromContext(ctx)
	manifests, err := WriteExpiryManifestsFromReader(ctx, c, expiryResultsReader)
	if err != nil {
		logger.WithError(err).Error("write per-bucket manifests for expiry: %s (no expiry performed)", err)
		return
	}
	type doneRec struct {
		bucketName string
		ok         bool
	}
	tagCh := make(chan doneRec)
	for bucketName, manifestReader := range manifests {
		manifestUrl := params.ManifestUrlForBucket(bucketName)
		bucketLogger := logger.WithFields(logging.Fields{"bucket": bucketName, "manifest_url": manifestUrl})
		bucketLogger.Info("start expiry on S3")
		go func(bucketName string, manifestReader io.ReadSeeker) {
			params := BatchTagOnS3BucketParams{
				AccountId:   params.AccountId,
				RoleArn:     params.RoleArn,
				BucketName:  bucketName,
				ManifestUrl: manifestUrl,
			}
			err := BatchTagOnS3Bucket(ctx, s3ControlClient, s3Client, manifestReader, &params)
			if err != nil {
				bucketLogger.WithError(err).Error("tag for expiry on S3")
				tagCh <- doneRec{bucketName: bucketName, ok: false}
			}
			tagCh <- doneRec{bucketName: bucketName, ok: true}
		}(bucketName, manifestReader)
	}

	taggedBuckets := make(map[string]struct{})
	for i := 0; i < len(manifests); i++ {
		done := <-tagCh
		if done.ok {
			taggedBuckets[done.bucketName] = struct{}{}
		}
	}

	// Filter entries from successful buckets
	err = expiryResultsReader.Rewind()
	if err != nil {
		logger.WithError(err).Error("[SEVERE] rewind expiry entries to expire on DB: entries may be lost")
		// TODO(ariels): attempt to cancel jobs?  (Tricky because those jobs are
		// unlikely already to be available -- meanwhile failing to rewind a file is
		// pretty much impossible.
		return
	}
	decoder := json.NewDecoder(expiryResultsReader)

	taggedRecordsByRepo := make(map[string] /*repositoryName*/ []*catalog.ExpireResult, 10000)
	for recordNumber := 0; ; recordNumber++ {
		recordLogger := logger.WithField("record_number", recordNumber)
		record := catalog.ExpireResult{}
		err = decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			recordLogger.WithError(err).Warning("failed to read record; keep going, already lost this expiry")
			continue
		}
		recordLogger = recordLogger.WithField("record", record)
		repository, err := c.GetRepository(ctx, record.Repository)
		if err != nil {
			recordLogger.WithError(err).Warning("failed to get repository URI; keep going, already lost this expiry")
			continue
		}
		qualifiedKey, err := block.ResolveNamespace(repository.StorageNamespace, record.PhysicalAddress)
		if err != nil {
			recordLogger.WithError(err).
				Warning("could not resolve namespace; keep going, already lost this expiry")
		}
		recordLogger = recordLogger.WithField("qualified_key", qualifiedKey)
		bucketName := qualifiedKey.StorageNamespace
		if _, ok := taggedBuckets[bucketName]; !ok {
			continue
		}
		taggedRecordsByRepo[record.Repository] = append(taggedRecordsByRepo[record.Repository], &record)
	}
	for repositoryName, records := range taggedRecordsByRepo {
		repositoryLogger := logger.WithFields(logging.Fields{"repository": repositoryName, "num_records": len(records)})
		err := c.MarkExpired(ctx, repositoryName, records)
		if err != nil {
			// TODO(ariels): attempt to cancel jobs?  (Tricky because those jobs are
			// unlikely already to be available.)
			repositoryLogger.WithError(err).Error("[SEVERE] failed to mark objects expired in catalog; S3 WILL expire them soon")
		} else {
			repositoryLogger.Info("marked objects expired in catalog")
		}
	}
}
