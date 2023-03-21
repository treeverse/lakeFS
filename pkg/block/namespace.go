package block

import (
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
	ValidityRegex          string // regex pattern that could be used to validate the namespace
	Example                string // example of a valid namespace
	DefaultNamespacePrefix string // when a repo is created from the UI, suggest a default storage namespace under this prefix
	PreSignSupport         bool
	PreSignSupportUI       bool
	ImportSupport          bool
}

type QualifiedKey interface {
	Format() string
	GetStorageType() StorageType
	GetStorageNamespace() string
	GetKey() string
}

type CommonQualifiedKey struct {
	StorageType      StorageType
	StorageNamespace string
	Key              string
}

func (qk CommonQualifiedKey) Format() string {
	return qk.StorageType.Scheme() + "://" + formatPathWithNamespace(qk.StorageNamespace, qk.Key)
}

func (qk CommonQualifiedKey) GetStorageType() StorageType {
	return qk.StorageType
}

func (qk CommonQualifiedKey) GetKey() string {
	return qk.Key
}

func (qk CommonQualifiedKey) GetStorageNamespace() string {
	return qk.StorageNamespace
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
		return st, fmt.Errorf("invalid storage scheme %s: %w", namespaceURL.Scheme, ErrInvalidAddress)
	}
}

func ValidateStorageType(uri *url.URL, expectedStorage StorageType) error {
	storage, err := GetStorageType(uri)
	if err != nil {
		return err
	}

	if storage != expectedStorage {
		return fmt.Errorf("expected storage type %s: %w", expectedStorage.Scheme(), ErrInvalidAddress)
	}
	return nil
}

func formatPathWithNamespace(namespacePath, keyPath string) string {
	namespacePath = strings.Trim(namespacePath, "/")
	if len(namespacePath) == 0 {
		return strings.TrimPrefix(keyPath, "/")
	}
	return namespacePath + "/" + keyPath
}

func DefaultResolveNamespace(defaultNamespace, key string, identifierType IdentifierType) (CommonQualifiedKey, error) {
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

func resolveFull(key string) (CommonQualifiedKey, error) {
	parsedKey, err := url.ParseRequestURI(key)
	if err != nil {
		return CommonQualifiedKey{}, fmt.Errorf("could not parse URI: %w", err)
	}
	// extract its scheme
	storageType, err := GetStorageType(parsedKey)
	if err != nil {
		return CommonQualifiedKey{}, err
	}
	return CommonQualifiedKey{
		StorageType:      storageType,
		StorageNamespace: parsedKey.Host,
		Key:              formatPathWithNamespace("", parsedKey.Path),
	}, nil
}

func resolveRelative(defaultNamespace, key string) (CommonQualifiedKey, error) {
	// is not fully qualified, treat as key only
	// if we don't have a trailing slash for the namespace, add it.
	parsedNS, err := url.ParseRequestURI(defaultNamespace)
	if err != nil {
		return CommonQualifiedKey{}, fmt.Errorf("default namespace %s: %w", defaultNamespace, ErrInvalidAddress)
	}
	storageType, err := GetStorageType(parsedNS)
	if err != nil {
		return CommonQualifiedKey{}, fmt.Errorf("no storage type for %s: %w", parsedNS, err)
	}

	return CommonQualifiedKey{
		StorageType:      storageType,
		StorageNamespace: strings.TrimSuffix(parsedNS.Host+parsedNS.Path, "/"),
		Key:              key,
	}, nil
}

func resolveNamespaceUnknown(defaultNamespace, key string) (CommonQualifiedKey, error) {
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
		ValidityRegex:  DefaultValidationRegex(scheme),
		Example:        DefaultExample(scheme),
		PreSignSupport: true,
		ImportSupport:  true,
	}
}
