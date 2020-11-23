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

const (
	defaultPriority = 10
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
func WriteExpiryManifestsFromRows(ctx context.Context, repository *catalog.Repository, rows catalog.StringIterator) (map[string]fileutil.RewindableReader, error) {
	logger := logging.FromContext(ctx)
	bucketWriters := BucketWriters{}

	recordNumber := 0
	for ; rows.Next(); recordNumber++ {
		recordLogger := logger.WithField("record_number", recordNumber)
		physicalAddress, err := rows.Read()
		if err != nil {
			recordLogger.WithError(err).Warning("failed to read record; keep going, lose this expiry")
			continue
		}
		recordLogger = recordLogger.WithField("physical_path", physicalAddress)
		recordLogger = recordLogger.WithField("repository", repository.Name)
		if !block.IsResolvableKey(physicalAddress) {
			recordLogger.Warning("expiry requested for non-resolvable key %s; ignore it (possible misconfiguration)", physicalAddress)
			continue
		}
		qualifiedKey, err := block.ResolveNamespace(repository.StorageNamespace, physicalAddress)
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
	logger = logger.WithFields(logging.Fields{"num_records": recordNumber, "num_files": len(bucketWriters)})
	logger.Info("encoded CSVs")
	if err := rows.Err(); err != nil && !errors.Is(err, io.EOF) {
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
	AccountID string

	// Role for performing tagging
	RoleArn string

	// Name of bucket holding objects to tag
	BucketName string

	// Path to use for manifest on S3, in "S3BatchOperations_CSV_20180820" format
	ManifestURL string

	// If present, S3 prefix for reporting
	ReportS3PrefixURL *string
}

type s3Pointer struct {
	Bucket string
	Key    string
}

func (sp s3Pointer) GetBucketArn() string {
	return fmt.Sprintf("arn:aws:s3:::%s", sp.Bucket)
}

func (sp s3Pointer) GetArn() string {
	return fmt.Sprintf("arn:aws:s3:::%s/%s", sp.Bucket, sp.Key)
}

func parseS3URL(s3URL string) (s3Pointer, error) {
	u, err := url.Parse(s3URL)
	if err != nil {
		return s3Pointer{}, fmt.Errorf("parse S3 URL %s: %w", s3URL, err)
	}
	if u.Scheme != "s3" {
		return s3Pointer{}, fmt.Errorf("URL %s not on S3: %w", s3URL, err)
	}
	trimmedPath := strings.TrimPrefix(u.Path, "/")
	return s3Pointer{
		Bucket: u.Host,
		Key:    trimmedPath,
	}, nil
}

// BatchTagOnS3Bucket uses client (which should be on the right region) to start an AWS S3 batch
// tagging operation in bucket according to the CSV contents of reader.
func BatchTagOnS3Bucket(ctx context.Context, s3ControlClient s3controliface.S3ControlAPI, s3Client s3iface.S3API, reader io.ReadSeeker, params *BatchTagOnS3BucketParams) error {
	manifestURL, err := parseS3URL(params.ManifestURL)
	if err != nil {
		return fmt.Errorf("manifest: %w", err)
	}
	logger := logging.FromContext(ctx).WithFields(logging.Fields{
		"manifest": params.ManifestURL,
		"bucket":   params.BucketName,
	})
	// Upload to S3 and get ETag.
	manifestParams := s3.PutObjectInput{
		Body:    reader,
		Bucket:  &manifestURL.Bucket,
		Key:     &manifestURL.Key,
		Tagging: aws.String("service=lakeFS&type=retention-manifest"),
	}
	upload, err := s3Client.PutObject(&manifestParams)
	if err != nil {
		return fmt.Errorf("putObject %+v: %w", manifestParams, err)
	}
	// PutObject includes _quotes_ around the etag.  Strip it.
	etag := strings.TrimSuffix(strings.TrimPrefix(*upload.ETag, "\""), "\"")
	logger.WithField("etag", etag).Info("Manifest uploaded")

	report := s3control.JobReport{
		Enabled: aws.Bool(false),
	}
	if params.ReportS3PrefixURL != nil {
		reportS3PrefixURL, err := parseS3URL(*params.ReportS3PrefixURL)
		if err != nil {
			return fmt.Errorf("report: %w", err)
		}
		report.SetEnabled(true)
		report.SetFormat(s3control.JobReportFormatReportCsv20180820)
		report.SetBucket(reportS3PrefixURL.GetBucketArn())
		report.SetPrefix(reportS3PrefixURL.Key)
		// TODO(ariels): Configure to report failed tasks only?
		report.SetReportScope(s3control.JobReportScopeAllTasks)
	}

	input := s3control.CreateJobInput{
		AccountId:            &params.AccountID,
		ConfirmationRequired: aws.Bool(false),
		// TODO(ariels): use ClientRequestToken to help avoid flooding?
		Description: aws.String("automated tag to expire objects"),
		Manifest: &s3control.JobManifest{
			Location: &s3control.JobManifestLocation{
				ETag:      aws.String(etag),
				ObjectArn: aws.String(manifestURL.GetArn()),
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
		Priority: aws.Int64(defaultPriority),
		// TODO(ariels): allow configuration of Report field
		Report:  &report,
		RoleArn: &params.RoleArn,
		Tags: []*s3control.S3Tag{
			{Key: aws.String("service"), Value: aws.String("lakeFS")},
			{Key: aws.String("job"), Value: aws.String("tag-expiry")},
		},
	}
	err = input.Validate()
	if err != nil {
		logger.WithField("input", input).WithError(err).Error("validate CreateJob input")
		return fmt.Errorf("validate CreateJobInput: %w", err)
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
	AccountID            string
	RoleArn              string
	ManifestURLForBucket func(string) string
	ReportS3PrefixURL    *string
}

// MarkedEntriesExpiredInChunks scans expiryResultsReader in chunks of up to chunkSize entries and
// marks all entries in each chunk as expired.  It writes all errors to errCh.
func MarkEntriesExpiredInChunks(ctx context.Context, c catalog.Cataloger, repositoryName string, expiryResultsReader io.Reader, chunkSize int, errCh chan error) {
	errFields := FromLoggerContext(ctx)

	decoder := json.NewDecoder(expiryResultsReader)
	done := false
	for recordNumber := 0; !done; {
		chunk := make([]*catalog.ExpireResult, 0, chunkSize)
		for i := 0; i < chunkSize; i++ {
			recordNumber++
			record := catalog.ExpireResult{}
			err := decoder.Decode(&record)
			if errors.Is(err, io.EOF) {
				done = true
				break
			}
			if err != nil {
				errCh <- MapError{errFields.WithField("record_number", recordNumber),
					fmt.Errorf("failed to read record: %w; keep going, already lost this expiry", err)}
			}
			chunk = append(chunk, &record)
		}
		if err := c.MarkEntriesExpired(ctx, repositoryName, chunk); err != nil {
			errCh <- MapError{errFields.WithFields(
				Fields{"chunk_size": len(chunk), "first_record_number": recordNumber - len(chunk)}),
				fmt.Errorf("failed to mark expired: %w", err),
			}
		}
	}
}

// ExpireOnS3 starts a goroutine to expire all entries on expiryResultsReader and returns a
// channel that will receive all error results.
func ExpireOnS3(ctx context.Context, s3ControlClient s3controliface.S3ControlAPI, s3Client s3iface.S3API, c catalog.Cataloger, repository *catalog.Repository, expiryResultsReader fileutil.RewindableReader, params *ExpireOnS3Params) chan error {
	const markExpiredChunkSize = 50000

	// TODO(ariels): Lock something for this "session" (e.g. process)

	const errChannelSize = 100
	errCh := make(chan error, errChannelSize)
	logger := logging.FromContext(ctx)
	errFields := FromLoggerContext(ctx)

	go func() {
		defer close(errCh)
		err := expiryResultsReader.Rewind()
		if err != nil {
			errCh <- MapError{errFields, fmt.Errorf("rewind expiry records file for marking entries: %w (no expiry performed)", err)}
			return
		}

		// Mark repository entries "is_expired", their contents are now invisible.  If
		// failed some entries won't be expired, so their files might not be deleted.
		// But this is safe, so keep going -- we may still succeed for the other
		// entries.
		MarkEntriesExpiredInChunks(ctx, c, repository.Name, expiryResultsReader, markExpiredChunkSize, errCh)

		// Mark object with no unexpired objects as intended for deletion, so no further
		// dedupes will occur on them.
		numToDelete, err := c.MarkObjectsForDeletion(ctx, repository.Name)
		if err != nil {
			errCh <- MapError{errFields, fmt.Errorf("mark deduped objects for deletion: %w", err)}
			// Unsafe to keep going (because "this cannot happen")
			return
		}
		logger = logger.WithField("num_objects_to_delete", numToDelete)
		errFields = errFields.WithField("num_objects_to_delete", numToDelete)
		logger.Info("Marked objects for deletion, start deleting where possible")

		// Now for each object marked for deletion, if *still* there are only _expired_
		// entries pointing at it then it is safe to delete.  Prepare the manifests and
		// start the batch taggers.
		expiryPhysicalAddressRows, err := c.DeleteOrUnmarkObjectsForDeletion(ctx, repository.Name)
		if err != nil {
			errCh <- MapError{errFields, fmt.Errorf("delete (or unmark) marked objects: %w", err)}
			return
		}

		manifests, err := WriteExpiryManifestsFromRows(ctx, repository, expiryPhysicalAddressRows)
		if err != nil {
			errCh <- MapError{
				Fields:       errFields,
				WrappedError: fmt.Errorf("write per-bucket manifests for expiry: %w (no expiry performed)", err),
			}
			return
		}

		type doneRec struct {
			bucketName string
			err        error
		}
		tagCh := make(chan doneRec)
		for bucketName, manifestReader := range manifests {
			manifestURL := params.ManifestURLForBucket(bucketName)
			bucketLogger := logger.WithFields(logging.Fields{"bucket": bucketName, "manifest_url": manifestURL})
			bucketFields := errFields.WithFields(Fields{"bucket": bucketName, "manifest_url": manifestURL})
			bucketLogger.Info("start expiry on S3")
			go func(bucketName string, manifestReader io.ReadSeeker) {
				params := BatchTagOnS3BucketParams{
					AccountID:         params.AccountID,
					RoleArn:           params.RoleArn,
					BucketName:        bucketName,
					ManifestURL:       manifestURL,
					ReportS3PrefixURL: params.ReportS3PrefixURL,
				}
				err := BatchTagOnS3Bucket(ctx, s3ControlClient, s3Client, manifestReader, &params)
				if err != nil {
					tagCh <- doneRec{
						bucketName: bucketName,
						err:        MapError{bucketFields, fmt.Errorf("tag for expiry on S3: %w", err)},
					}
				}
				tagCh <- doneRec{bucketName: bucketName}
			}(bucketName, manifestReader)
		}

		taggedBuckets := make(map[string]struct{})
		for i := 0; i < len(manifests); i++ {
			done := <-tagCh
			if done.err != nil {
				errCh <- done.err
				continue
			}
			taggedBuckets[done.bucketName] = struct{}{}
		}
	}()
	return errCh
}
