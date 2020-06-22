package block_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/treeverse/lakefs/block"
)

func TestResolveNamespace(t *testing.T) {
	cases := []struct {
		Name             string
		DefaultNamespace string
		Key              string
		ExpectedErr      error
		Expected         block.QualifiedKey
	}{
		{
			Name:             "valid_namespace_no_trailing_slash",
			DefaultNamespace: "s3://foo",
			Key:              "bar/baz",
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
			ExpectedErr:      block.ErrInvalidNamespace,
			Expected:         block.QualifiedKey{},
		},
		{
			Name:             "invalid_namespace_invalid_uri",
			DefaultNamespace: "foo",
			Key:              "bar/baz",
			ExpectedErr:      block.ErrInvalidNamespace,
			Expected:         block.QualifiedKey{},
		},
		{
			Name:             "invalid_key_wrong_scheme",
			DefaultNamespace: "s3://foo/",
			Key:              "s4://bar/baz",
			ExpectedErr:      block.ErrInvalidNamespace,
			Expected:         block.QualifiedKey{},
		},
	}

	for _, cas := range cases {
		t.Run(cas.Name, func(t *testing.T) {
			resolved, err := block.ResolveNamespace(cas.DefaultNamespace, cas.Key)
			if err != nil && !errors.Is(err, cas.ExpectedErr) {
				t.Fatalf("got unexpected error :%v - expected %v", err, cas.ExpectedErr)
			}
			if cas.ExpectedErr == nil && !reflect.DeepEqual(resolved, cas.Expected) {
				t.Fatalf("expected %v got %v", cas.Expected, resolved)
			}
		})
	}
}
