package block

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

type StorageType int

const (
	StorageTypeMem = iota
	StorageTypeLocal
	StorageTypeS3
)

var (
	ErrInvalidNamespace = errors.New("invalid namespace")
)

type QualifiedKey struct {
	StorageType      StorageType
	StorageNamespace string
	Key              string
}

func GetStorageType(namespaceURL *url.URL) (StorageType, error) {
	var st StorageType
	switch namespaceURL.Scheme {
	case "s3":
		return StorageTypeS3, nil
	case "mem", "memory":
		return StorageTypeMem, nil
	case "local":
		return StorageTypeLocal, nil
	default:
		return st, fmt.Errorf("%s: %w", namespaceURL.Scheme, ErrInvalidNamespace)
	}
}

func formatPathWithNamespace(namespacePath, keyPath string) string {
	namespacePath = strings.TrimPrefix(namespacePath, "/")
	keyPath = strings.TrimPrefix(keyPath, "/")
	if len(namespacePath) == 0 {
		return keyPath
	}
	return namespacePath + "/" + keyPath
}

// IsResolvableKey returns true if a key will be resolved into the default namespace of its
// bucket.  Resolvable keys are subject to expiry, so it errs on the side of calling keys *not*
// resolvable.
func IsResolvableKey(key string) bool {
	_, err := url.ParseRequestURI(key)
	return err != nil
}

func ResolveNamespace(defaultNamespace, key string) (QualifiedKey, error) {
	// check if the key is fully qualified
	parsedKey, err := url.ParseRequestURI(key)
	var qk QualifiedKey
	if err != nil {
		// is not fully qualified, treat as key only
		// if we don't have a trailing slash for the namespace, add it.
		parsedNs, err := url.ParseRequestURI(defaultNamespace)
		if err != nil {
			return qk, fmt.Errorf("default namespace %s: %w", defaultNamespace, ErrInvalidNamespace)
		}
		storageType, err := GetStorageType(parsedNs)
		if err != nil {
			return qk, err
		}

		return QualifiedKey{
			StorageType:      storageType,
			StorageNamespace: parsedNs.Host,
			Key:              formatPathWithNamespace(parsedNs.Path, key),
		}, nil
	}

	// extract its scheme
	storageType, err := GetStorageType(parsedKey)
	if err != nil {
		return qk, err
	}
	return QualifiedKey{
		StorageType:      storageType,
		StorageNamespace: parsedKey.Host,
		Key:              formatPathWithNamespace("", parsedKey.Path),
	}, nil
}
