package fileutil

import (
	"testing"
)

func TestWriterThenReader(t *testing.T) {
	writer, err := NewFileWriterThenReader("testing-*.tmp")
	if err != nil {
		t.Fatalf("opening fileWriterThenReader: %s", err)
	}

	text := []byte("the quick brown fox jumps over the lazy dog. ")

	const count = 100000
	for i := 0; i < count; i++ {
		l, err := writer.Write(text)
		if err != nil {
			t.Fatalf("writing block %d: %s", i, err)
		}
		if l != len(text) {
			t.Fatalf("writing block %d: passed %d but wrote %d", i, len(text), l)
		}
	}

	// Re-read data
	reader, length, err := writer.StartReading()
	if err != nil {
		t.Fatalf("start reading fileWriterThenReader: %s", err)
	}
	t.Log("total length", length, "reader", reader)
	for i := 0; i < count; i++ {
		p := make([]byte, len(text))
		l, err := reader.Read(p)
		if err != nil {
			t.Fatalf("failed to read item %d: %s", i, err)
		}
		if l != len(text) {
			t.Fatalf("writing block %d: passed %d but wrote %d", i, len(text), l)
		}
	}

	// Re-read data again
	err = reader.Rewind()
	if err != nil {
		t.Fatalf("rewind fileWriterThenReader: %s", err)
	}
	for i := 0; i < count; i++ {
		p := make([]byte, len(text))
		l, err := reader.Read(p)
		if err != nil {
			t.Fatalf("failed to read item %d: %s", i, err)
		}
		if l != len(text) {
			t.Fatalf("writing block %d: passed %d but wrote %d", i, len(text), l)
		}
	}
}
