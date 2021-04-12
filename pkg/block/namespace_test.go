package block_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/pkg/block"
)

func TestResolveNamespace(t *testing.T) {
	cases := []struct {
		Name             string
		DefaultNamespace string
		Key              string
		Type             block.IdentifierType
		ExpectedErr      error
		Expected         block.QualifiedKey
	}{
		{
			Name:             "valid_namespace_no_trailing_slash",
			DefaultNamespace: "s3://foo",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.QualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "foo",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_namespace_with_trailing_slash",
			DefaultNamespace: "s3://foo/",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.QualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "foo",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_namespace_mem_with_trailing_slash",
			DefaultNamespace: "mem://foo/",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      nil,
			Expected: block.QualifiedKey{
				StorageType:      block.StorageTypeMem,
				StorageNamespace: "foo",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "valid_fq_key",
			DefaultNamespace: "mem://foo/",
			Key:              "s3://example/bar/baz",
			Type:             block.IdentifierTypeFull,
			ExpectedErr:      nil,
			Expected: block.QualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "example",
				Key:              "bar/baz",
			},
		},
		{
			Name:             "invalid_namespace_wrong_scheme",
			DefaultNamespace: "memzzzz://foo/",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      block.ErrInvalidNamespace,
			Expected:         block.QualifiedKey{},
		},
		{
			Name:             "invalid_namespace_invalid_uri",
			DefaultNamespace: "foo",
			Key:              "bar/baz",
			Type:             block.IdentifierTypeRelative,
			ExpectedErr:      block.ErrInvalidNamespace,
			Expected:         block.QualifiedKey{},
		},
		{
			Name:             "invalid_key_wrong_scheme",
			DefaultNamespace: "s3://foo/",
			Key:              "s4://bar/baz",
			Type:             block.IdentifierTypeFull,
			ExpectedErr:      block.ErrInvalidNamespace,
			Expected:         block.QualifiedKey{},
		},
		{
			Name:             "key_weird_format",
			DefaultNamespace: "s3://foo/",
			Key:              "://invalid/baz",
			Type:             block.IdentifierTypeRelative,
			Expected: block.QualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "foo",
				Key:              "://invalid/baz",
			},
		},
	}

	for _, cas := range cases {
		for _, r := range []block.IdentifierType{cas.Type, block.IdentifierTypeUnknownDeprecated} {
			relativeName := ""
			switch r {
			case block.IdentifierTypeUnknownDeprecated:
				relativeName = "unknown"
			case block.IdentifierTypeRelative:
				relativeName = "relative"
			case block.IdentifierTypeFull:
				relativeName = "full"
			}
			t.Run(fmt.Sprintf("%s/%s", cas.Name, relativeName), func(t *testing.T) {
				resolved, err := block.ResolveNamespace(cas.DefaultNamespace, cas.Key, r)
				if err != nil && !errors.Is(err, cas.ExpectedErr) {
					t.Fatalf("got unexpected error :%v - expected %v", err, cas.ExpectedErr)
				}
				if cas.ExpectedErr == nil && !reflect.DeepEqual(resolved, cas.Expected) {
					t.Fatalf("expected %v got %v", cas.Expected, resolved)
				}
			})
		}

	}
}

func TestFormatQualifiedKey(t *testing.T) {
	cases := []struct {
		Name         string
		QualifiedKey block.QualifiedKey
		Expected     string
	}{
		{
			Name: "simple_path",
			QualifiedKey: block.QualifiedKey{
				StorageType:      block.StorageTypeGS,
				StorageNamespace: "some-bucket",
				Key:              "path",
			},
			Expected: "gs://some-bucket/path",
		},
		{
			Name: "path_with_prefix",
			QualifiedKey: block.QualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "some-bucket/",
				Key:              "/path/to/file",
			},
			Expected: "s3://some-bucket/path/to/file",
		},
		{
			Name: "bucket_with_prefix",
			QualifiedKey: block.QualifiedKey{
				StorageType:      block.StorageTypeS3,
				StorageNamespace: "some-bucket/prefix/",
				Key:              "/path/to/file",
			},
			Expected: "s3://some-bucket/prefix/path/to/file",
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			formatted := cas.QualifiedKey.Format()
			if formatted != cas.Expected {
				t.Fatalf("expected %v got %v", cas.Expected, formatted)
			}
		})
	}
}
