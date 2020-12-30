package retention_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/treeverse/lakefs/retention"
)

func TestFields_WithField(t *testing.T) {
	fields := retention.Fields{}.WithField("a", 1)
	if fields["a"].(int) != 1 {
		t.Errorf("expected to set field \"a\" to 1, got %v", fields)
	}
	moreFields := fields.WithField("b", "two")
	if moreFields["b"].(string) != "two" {
		t.Errorf("expected to set field \"b\" to \"two\", got %v", moreFields)
	}
	if moreFields["a"].(int) != 1 {
		t.Errorf("expected to keep field \"a\" at 1 after WithFields(\"b\", ...), got %v", moreFields)
	}
	if _, ok := fields["b"]; ok {
		t.Errorf("expected WithFields(\"b\", ...) not to change original fields, got %v", fields)
	}
}

func TestField_WithFields(t *testing.T) {
	fieldsA := retention.Fields{}.WithField("a", 1)
	fieldsB := retention.Fields{}.WithField("b", "two")
	fields := fieldsA.WithFields(fieldsB)

	if _, ok := fields["a"]; !ok {
		t.Errorf("expected field \"a\" on merged fields, got %v", fields)
	}
	if _, ok := fields["b"]; !ok {
		t.Errorf("expected field \"b\" on merged fields, got %v", fields)
	}
	if _, ok := fieldsA["b"]; ok {
		t.Errorf("expected WithFields(...) not to change original fields, got %v", fieldsA)
	}
	if _, ok := fieldsB["a"]; ok {
		t.Errorf("expected WithFields(...) not to change argument fields, got %v", fieldsB)
	}
}

func TestField_String(t *testing.T) {
	fields := retention.Fields{}.WithField("foo", "xyzzy").WithField("bar", 22)
	s := fields.String()
	if s != "bar=22, foo=xyzzy" {
		t.Errorf("unexpected string representation of %v: %s", fields, s)
	}
}

func TestMapError(t *testing.T) {
	errTesting := fmt.Errorf("error for testing")
	err := retention.MapError{Fields: retention.Fields{"a": 1, "b": 2}, WrappedError: errTesting}
	if !errors.Is(err, errTesting) {
		t.Errorf("error %s failed to wrap its base %s", err, errTesting)
	}
}
