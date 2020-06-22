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

func GetStorageType(namespaceUrl *url.URL) (StorageType, error) {
	var st StorageType
	switch namespaceUrl.Scheme {
	case "s3":
		return StorageTypeS3, nil
	case "mem", "memory":
		return StorageTypeMem, nil
	case "local":
		return StorageTypeLocal, nil
	default:
		return st, fmt.Errorf("%s: %w", namespaceUrl.Scheme, ErrInvalidNamespace)
	}
}

func pathJoin(s1, s2 string) string {
	s1 = strings.TrimPrefix(s1, "/")
	s2 = strings.TrimPrefix(s2, "/")
	if len(s1) == 0 {
		return s2
	}
	return s1 + "/" + s2
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
			Key:              pathJoin(parsedNs.Path, key),
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
		Key:              pathJoin("", parsedKey.Path),
	}, nil
}
