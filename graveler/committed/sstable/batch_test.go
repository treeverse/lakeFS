package sstable

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/treeverse/lakefs/graveler/committed"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
	"github.com/treeverse/lakefs/graveler/committed/mock"
)

func TestBatchCloserSuccess(t *testing.T) {
	runSuccessScenario(t)
}

func TestBatchWriterFailed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writerSuccess := mock.NewMockWriter(ctrl)
	writerSuccess.EXPECT().Close().Return(&committed.WriteResult{
		PartID: committed.ID(strconv.Itoa(1)),
		First:  committed.Key("row_1"),
		Last:   committed.Key("row_2"),
		Count:  4321,
	}, nil).Times(1)
	writerFailure := mock.NewMockWriter(ctrl)
	expectedErr := errors.New("failure")
	writerFailure.EXPECT().Close().Return(nil, expectedErr).Times(1)

	sut := NewBatchCloser()
	require.NoError(t, sut.CloseWriterAsync(writerSuccess))
	require.NoError(t, sut.CloseWriterAsync(writerFailure))

	res, err := sut.Wait()
	require.Error(t, expectedErr, err)
	require.Nil(t, res)
}

func TestBatchCloserMultipleWaitCalls(t *testing.T) {
	sut, ctrl := runSuccessScenario(t)

	writer := mock.NewMockWriter(ctrl)
	writer.EXPECT().Close().Return(&committed.WriteResult{
		PartID: committed.ID("last"),
		First:  committed.Key("row_1"),
		Last:   committed.Key("row_2"),
		Count:  4321,
	}, nil).Times(1)

	require.Error(t, sut.CloseWriterAsync(writer), errMultipleWaitCalls)
	res, err := sut.Wait()
	require.Nil(t, res)
	require.Error(t, err, errMultipleWaitCalls)
}

func runSuccessScenario(t *testing.T) (*BatchCloser, *gomock.Controller) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	writersCount := 10
	writers := make([]*mock.MockWriter, writersCount, writersCount)
	for i := 0; i < writersCount; i++ {
		writers[i] = mock.NewMockWriter(ctrl)
		writers[i].EXPECT().Close().Return(&committed.WriteResult{
			PartID: committed.ID(strconv.Itoa(i)),
			First:  committed.Key(fmt.Sprintf("row_%d_1", i)),
			Last:   committed.Key(fmt.Sprintf("row_%d_2", i)),
			Count:  i,
		}, nil).Times(1)
	}

	sut := NewBatchCloser()

	for i := 0; i < writersCount; i++ {
		require.NoError(t, sut.CloseWriterAsync(writers[i]))
	}

	res, err := sut.Wait()
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res, writersCount)

	return sut, ctrl
}
