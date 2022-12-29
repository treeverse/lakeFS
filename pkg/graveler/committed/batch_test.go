package committed_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/go-test/deep"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/pkg/graveler"
	"github.com/treeverse/lakefs/pkg/graveler/committed"
)

// FakeRangeWriter is a RangeWriter that is safe to use in goroutines.  (mock.RangeWriter is
// NOT safe, it uses gomock which calls t.Fatal and friends!)
type FakeRangeWriter struct {
	err error

	closed       bool
	closeResult  closeResult
	writeRecords []*committed.Record

	storedType string
}

type closeResult struct {
	result *committed.WriteResult
	err    error
}

var (
	ErrAlreadyClosed = errors.New("closed more than once")
	ErrUnexpected    = errors.New("unexpected call")
)

func (f *FakeRangeWriter) Err() error {
	return f.err
}

func (f *FakeRangeWriter) setErr(err error) error {
	f.err = err
	return err
}

func (f *FakeRangeWriter) ExpectWriteRecord(r committed.Record) {
	f.writeRecords = append(f.writeRecords, &r)
}

func (f *FakeRangeWriter) ExpectAnyRecord() {
	f.writeRecords = append(f.writeRecords, nil)
}

func (f *FakeRangeWriter) WriteRecord(r committed.Record) error {
	if len(f.writeRecords) < 1 {
		return f.setErr(fmt.Errorf("try to write %+v when expected nothing: %w", r, ErrUnexpected))
	}
	var n *committed.Record
	n, f.writeRecords = f.writeRecords[0], f.writeRecords[1:]
	if n == nil { // any
		return nil
	}
	if diffs := deep.Equal(&r, n); diffs != nil {
		return f.setErr(fmt.Errorf("try to write the wrong value %s: %w", diffs, ErrUnexpected))
	}
	return nil
}

func (f *FakeRangeWriter) SetMetadata(key, value string) {
	if key == committed.MetadataTypeKey {
		f.storedType = value
	}
}

func (*FakeRangeWriter) GetApproximateSize() uint64 { return 0 }

func (*FakeRangeWriter) ShouldBreakAtKey(graveler.Key, *committed.Params) bool { return false }

func (f *FakeRangeWriter) Close() (*committed.WriteResult, error) {
	if f.closed {
		f.err = ErrAlreadyClosed
		return nil, ErrAlreadyClosed
	}
	return f.closeResult.result, f.closeResult.err
}

func (f *FakeRangeWriter) Abort() error { return nil }

func NewFakeRangeWriter(result *committed.WriteResult, err error) *FakeRangeWriter {
	return &FakeRangeWriter{
		closeResult: closeResult{result, err},
	}
}

func TestBatchCloserSuccess(t *testing.T) {
	runSuccessScenario(t, 502, 5)
}

func TestBatchWriterFailed(t *testing.T) {
	writerSuccess := NewFakeRangeWriter(
		&committed.WriteResult{
			RangeID: committed.ID(strconv.Itoa(1)),
			First:   committed.Key("row_1"),
			Last:    committed.Key("row_2"),
			Count:   4321,
		}, nil)
	expectedErr := errors.New("failure")
	writerFailure := NewFakeRangeWriter(nil, expectedErr)

	sut := committed.NewBatchCloser(10)
	assert.NoError(t, sut.CloseWriterAsync(writerSuccess))
	assert.NoError(t, sut.CloseWriterAsync(writerFailure))

	res, err := sut.Wait()
	assert.Error(t, expectedErr, err)
	assert.Nil(t, res)

	assert.NoError(t, writerSuccess.Err())
	assert.NoError(t, writerFailure.Err())
}

func TestBatchCloserMultipleWaitCalls(t *testing.T) {
	writer := NewFakeRangeWriter(&committed.WriteResult{
		RangeID: "last",
		First:   committed.Key("row_1"),
		Last:    committed.Key("row_2"),
		Count:   4321,
	}, nil)

	sut := runSuccessScenario(t, 1, 1)

	assert.Error(t, sut.CloseWriterAsync(writer), committed.ErrMultipleWaitCalls)
	res, err := sut.Wait()
	require.Nil(t, res)
	require.Error(t, err, committed.ErrMultipleWaitCalls)
}

func runSuccessScenario(t *testing.T, numWriters, numClosers int) *committed.BatchCloser {
	t.Helper()

	writers := make([]*FakeRangeWriter, numWriters)
	for i := 0; i < numWriters; i++ {
		writers[i] = NewFakeRangeWriter(&committed.WriteResult{
			RangeID: committed.ID(strconv.Itoa(i)),
			First:   committed.Key(fmt.Sprintf("row_%d_1", i)),
			Last:    committed.Key(fmt.Sprintf("row_%d_2", i)),
			Count:   i,
		}, nil)
	}

	sut := committed.NewBatchCloser(numClosers)

	for i := 0; i < numWriters; i++ {
		assert.NoError(t, sut.CloseWriterAsync(writers[i]))
	}

	res, err := sut.Wait()
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Len(t, res, numWriters)

	return sut
}
