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
	StorageTypeGS
	StorageTypeAzure
)

var ErrInvalidNamespace = errors.New("invalid namespace")

func (s StorageType) BlockstoreType() string {
	switch s {
	case StorageTypeAzure:
		return "azure"
	default:
		return s.Scheme()
	}
}

func (s StorageType) Scheme() string {
	scheme := ""
	switch s {
	case StorageTypeMem:
		scheme = "mem"
	case StorageTypeLocal:
		scheme = "local"
	case StorageTypeGS:
		scheme = "gs"
	case StorageTypeS3:
		scheme = "s3"
	case StorageTypeAzure:
		scheme = "https"
	default:
		panic("unknown storage type")
	}
	return scheme
}

type StorageNamespaceInfo struct {
	ValidityRegex string // regex pattern that could be used to validate the namespace
	Example       string // example of a valid namespace
}

type QualifiedKey struct {
	StorageType      StorageType
	StorageNamespace string
	Key              string
}

type QualifiedPrefix struct {
	StorageType      StorageType
	StorageNamespace string
	Prefix           string
}

func (qk QualifiedKey) Format() string {
	return qk.StorageType.Scheme() + "://" + formatPathWithNamespace(qk.StorageNamespace, qk.Key)
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
	case "gs":
		return StorageTypeGS, nil
	case "http", "https":
		return StorageTypeAzure, nil
	default:
		return st, fmt.Errorf("%s: %w", namespaceURL.Scheme, ErrInvalidNamespace)
	}
}

func formatPathWithNamespace(namespacePath, keyPath string) string {
	namespacePath = strings.Trim(namespacePath, "/")
	if len(namespacePath) == 0 {
		return strings.TrimPrefix(keyPath, "/")
	}
	return namespacePath + "/" + keyPath
}

func ResolveNamespacePrefix(defaultNamespace, prefix string) (QualifiedPrefix, error) {
	// behaviour for key and prefix is the same
	key, err := resolveRelative(defaultNamespace, prefix)
	if err != nil {
		return QualifiedPrefix{}, fmt.Errorf("resolving namespace: %w", err)
	}

	return QualifiedPrefix{
		StorageType:      key.StorageType,
		StorageNamespace: key.StorageNamespace,
		Prefix:           key.Key,
	}, nil
}

func ResolveNamespace(defaultNamespace, key string, identifierType IdentifierType) (QualifiedKey, error) {
	switch identifierType {
	case IdentifierTypeUnknownDeprecated:
		return resolveNamespaceUnknown(defaultNamespace, key)
	case IdentifierTypeRelative:
		return resolveRelative(defaultNamespace, key)
	case IdentifierTypeFull:
		return resolveFull(key)
	default:
		panic(fmt.Sprintf("unknown identifier type: %d", identifierType))
	}
}

func resolveFull(key string) (QualifiedKey, error) {
	parsedKey, err := url.ParseRequestURI(key)
	if err != nil {
		return QualifiedKey{}, fmt.Errorf("could not parse URI: %w", err)
	}
	// extract its scheme
	storageType, err := GetStorageType(parsedKey)
	if err != nil {
		return QualifiedKey{}, err
	}
	return QualifiedKey{
		StorageType:      storageType,
		StorageNamespace: parsedKey.Host,
		Key:              formatPathWithNamespace("", parsedKey.Path),
	}, nil
}

func resolveRelative(defaultNamespace, key string) (QualifiedKey, error) {
	// is not fully qualified, treat as key only
	// if we don't have a trailing slash for the namespace, add it.
	parsedNs, err := url.ParseRequestURI(defaultNamespace)
	if err != nil {
		return QualifiedKey{}, fmt.Errorf("default namespace %s: %w", defaultNamespace, ErrInvalidNamespace)
	}
	storageType, err := GetStorageType(parsedNs)
	if err != nil {
		return QualifiedKey{}, fmt.Errorf("no storage type for %s: %w", parsedNs, err)
	}

	return QualifiedKey{
		StorageType:      storageType,
		StorageNamespace: parsedNs.Host,
		Key:              formatPathWithNamespace(parsedNs.Path, key),
	}, nil
}

func resolveNamespaceUnknown(defaultNamespace, key string) (QualifiedKey, error) {
	// first try to treat key as a full path
	if qk, err := resolveFull(key); err == nil {
		return qk, nil
	}

	// else, treat it as a relative path
	return resolveRelative(defaultNamespace, key)
}

func DefaultExample(scheme string) string {
	return scheme + "://example-bucket/"
}

func DefaultValidationRegex(scheme string) string {
	return fmt.Sprintf("^%s://", scheme)
}

func DefaultStorageNamespaceInfo(scheme string) StorageNamespaceInfo {
	return StorageNamespaceInfo{
		ValidityRegex: DefaultValidationRegex(scheme),
		Example:       DefaultExample(scheme),
	}
}
