package sstable_test

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/treeverse/lakefs/graveler/committed"
	"github.com/treeverse/lakefs/graveler/committed/mock"
	"github.com/treeverse/lakefs/graveler/sstable"
)

func TestBatchCloserSuccess(t *testing.T) {
	runSuccessScenario(t)
}

func TestBatchWriterFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writerSuccess := mock.NewMockRangeWriter(ctrl)
	writerSuccess.EXPECT().Close().Return(&committed.WriteResult{
		RangeID: committed.ID(strconv.Itoa(1)),
		First:   committed.Key("row_1"),
		Last:    committed.Key("row_2"),
		Count:   4321,
	}, nil).Times(1)
	writerFailure := mock.NewMockRangeWriter(ctrl)
	expectedErr := errors.New("failure")
	writerFailure.EXPECT().Close().Return(nil, expectedErr).Times(1)

	sut := sstable.NewBatchCloser()
	require.NoError(t, sut.CloseWriterAsync(writerSuccess))
	require.NoError(t, sut.CloseWriterAsync(writerFailure))

	res, err := sut.Wait()
	require.Error(t, expectedErr, err)
	require.Nil(t, res)
}

func TestBatchCloserMultipleWaitCalls(t *testing.T) {
	sut, ctrl := runSuccessScenario(t)

	writer := mock.NewMockRangeWriter(ctrl)
	writer.EXPECT().Close().Return(&committed.WriteResult{
		RangeID: "last",
		First:   committed.Key("row_1"),
		Last:    committed.Key("row_2"),
		Count:   4321,
	}, nil).Times(1)

	require.Error(t, sut.CloseWriterAsync(writer), sstable.ErrMultipleWaitCalls)
	res, err := sut.Wait()
	require.Nil(t, res)
	require.Error(t, err, sstable.ErrMultipleWaitCalls)
}

func runSuccessScenario(t *testing.T) (*sstable.BatchCloser, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	const writersCount = 10
	writers := make([]*mock.MockRangeWriter, writersCount)
	for i := 0; i < writersCount; i++ {
		writers[i] = mock.NewMockRangeWriter(ctrl)
		writers[i].EXPECT().Close().Return(&committed.WriteResult{
			RangeID: committed.ID(strconv.Itoa(i)),
			First:   committed.Key(fmt.Sprintf("row_%d_1", i)),
			Last:    committed.Key(fmt.Sprintf("row_%d_2", i)),
			Count:   i,
		}, nil).Times(1)
	}

	sut := sstable.NewBatchCloser()

	for i := 0; i < writersCount; i++ {
		require.NoError(t, sut.CloseWriterAsync(writers[i]))
	}

	res, err := sut.Wait()
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res, writersCount)

	return sut, ctrl
}
