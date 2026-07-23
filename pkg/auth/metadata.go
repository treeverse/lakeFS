package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
)

const (
	InstallationIDKeyName  = "installation_id"
	SetupTimestampKeyName  = "setup_timestamp"
	SetupAuthTypeKeyPrefix = "setup_auth_"
	CommPrefsSetKeyName    = "comm_prefs_set"
	EmailKeyName           = "encoded_user_email"
	FirstNameKeyName       = "encoded_user_first_name"
	LastNameKeyName        = "encoded_user_last_name"
	CompanyNameKeyName     = "encoded_user_company_name"
	FeatureUpdatesKeyName  = "feature_updates"

	InstrumentationSamplesRepo = "SamplesRepo"
	InstrumentationQuickstart  = "Quickstart"
	InstrumentationRun         = "Run"
)

type SetupStateName string

const (
	SetupStateInitialized    SetupStateName = "initialized"
	SetupStateNotInitialized SetupStateName = "not_initialized"
)

//nolint:gochecknoinits
func init() {
	kv.MustRegisterType("auth", InstallationIDKeyName, nil)
	kv.MustRegisterType("auth", SetupTimestampKeyName, nil)
}

type MetadataManager interface {
	IsInitialized(ctx context.Context) (bool, error)
	GetSetupState(ctx context.Context) (SetupStateName, error)
	UpdateCommPrefs(ctx context.Context, commPrefs *CommPrefs) (string, error)
	SetCommPrefsOnce(ctx context.Context, commPrefs *CommPrefs) (string, error)
	IsCommPrefsSet(ctx context.Context) (bool, error)
	UpdateSetupTimestamp(ctx context.Context, setupTime time.Time, authType string) error
	GetMetadata(context.Context) (map[string]string, error)
}

type KVMetadataManager struct {
	version        string
	kvType         string
	installationID string
	store          kv.Store

	// cache of positive setup/comm-prefs state to avoid hitting KV on every
	// check. These states only ever transition false->true, so a cached true
	// is correct indefinitely.
	initialized  atomic.Bool
	commPrefsSet atomic.Bool
}

type CommPrefs struct {
	UserEmail      string
	FirstName      string
	LastName       string
	CompanyName    string
	FeatureUpdates bool
	InstallationID string
}

func NewKVMetadataManager(version, fixedInstallationID, kvType string, store kv.Store) *KVMetadataManager {
	return &KVMetadataManager{
		version:        version,
		kvType:         kvType,
		installationID: generateInstallationID(fixedInstallationID),
		store:          store,
	}
}

func generateInstallationID(installationID string) string {
	if installationID == "" {
		installationID = uuid.New().String()
	}
	return installationID
}

func (m *KVMetadataManager) insertOrGetInstallationID(ctx context.Context, installationID string) (string, error) {
	err := m.store.SetIf(ctx, []byte(model.PartitionKey), []byte(InstallationIDKeyName), []byte(installationID), nil)
	if err != nil {
		if errors.Is(err, kv.ErrPredicateFailed) {
			valWithPred, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(InstallationIDKeyName))
			if err != nil {
				return "", err
			}
			return string(valWithPred.Value), nil
		}
		return "", err
	}
	return installationID, nil
}

func (m *KVMetadataManager) getSetupTimestamp(ctx context.Context) (time.Time, error) {
	valWithPred, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(SetupTimestampKeyName)))
	if err != nil {
		return time.Time{}, err
	}
	return time.Parse(time.RFC3339, string(valWithPred.Value))
}

func (m *KVMetadataManager) IsCommPrefsSet(ctx context.Context) (bool, error) {
	if m.commPrefsSet.Load() {
		return true, nil
	}

	commPrefsSet, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(CommPrefsSetKeyName)))
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return false, ErrNotFound
		}
		return false, err
	}

	isSet, err := strconv.ParseBool(string(commPrefsSet.Value))
	if err != nil {
		return false, err
	}
	// cache the result in case it is true
	if isSet {
		m.commPrefsSet.Store(isSet)
	}
	return isSet, nil
}

func (m *KVMetadataManager) GetSetupState(ctx context.Context) (SetupStateName, error) {
	// for backwards compatibility (i.e. so existing instances don't need to go through setup again)
	// we first check if the setup timestamp has already been set
	isInitialized, err := m.IsInitialized(ctx)
	if err != nil {
		return "", err
	}
	if isInitialized {
		return SetupStateInitialized, nil
	}
	return SetupStateNotInitialized, nil
}

func (m *KVMetadataManager) writeMetadata(ctx context.Context, items map[string]string) error {
	for key, value := range items {
		kvKey := model.MetadataKeyPath(key)
		err := m.store.Set(ctx, []byte(model.PartitionKey), []byte(kvKey), []byte(value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *KVMetadataManager) UpdateSetupTimestamp(ctx context.Context, setupTime time.Time, authType string) error {
	setupTimeStr := setupTime.UTC().Format(time.RFC3339)
	items := map[string]string{
		SetupTimestampKeyName: setupTimeStr,
	}
	items[SetupAuthTypeKeyPrefix+authType] = setupTimeStr
	err := m.writeMetadata(ctx, items)
	if err != nil {
		return err
	}
	// setup just completed; prime the cache so subsequent checks skip KV
	m.initialized.Store(true)
	return nil
}

// commPrefsData returns the (non-flag) metadata key/values for the given prefs.
func commPrefsData(commPrefs *CommPrefs) map[string]string {
	return map[string]string{
		FirstNameKeyName:      base64.StdEncoding.EncodeToString([]byte(commPrefs.FirstName)),
		LastNameKeyName:       base64.StdEncoding.EncodeToString([]byte(commPrefs.LastName)),
		EmailKeyName:          base64.StdEncoding.EncodeToString([]byte(commPrefs.UserEmail)),
		CompanyNameKeyName:    base64.StdEncoding.EncodeToString([]byte(commPrefs.CompanyName)),
		FeatureUpdatesKeyName: strconv.FormatBool(commPrefs.FeatureUpdates),
	}
}

// SetCommPrefsOnce persists comm prefs and marks them set, but only if they
// were not already set. It returns ErrCommPrefsAlreadySet otherwise, so a
// repeat call never overrides an established record.
//
// The prefs data is written first, and comm_prefs_set is flipped last with a
// conditional SetIf as the commit point. Ordering it this way means a failed
// data write leaves the flag unset, so a retry can still complete the record
// (no partial-write brick). Only one caller wins the SetIf commit; the rest get
// ErrCommPrefsAlreadySet. Concurrent callers that all observe the flag unset do
// race on the data itself (last write wins), which is acceptable.
func (m *KVMetadataManager) SetCommPrefsOnce(ctx context.Context, commPrefs *CommPrefs) (string, error) {
	if m.commPrefsSet.Load() {
		return m.installationID, ErrCommPrefsAlreadySet
	}

	flagKey := []byte(model.MetadataKeyPath(CommPrefsSetKeyName))
	current, err := m.store.Get(ctx, []byte(model.PartitionKey), flagKey)
	var predicate kv.Predicate
	switch {
	case errors.Is(err, kv.ErrNotFound):
		predicate = nil // commit only if the flag is still absent
	case err != nil:
		return m.installationID, err
	default:
		isSet, parseErr := strconv.ParseBool(string(current.Value))
		if parseErr != nil {
			return m.installationID, parseErr
		}
		if isSet {
			m.commPrefsSet.Store(true)
			return m.installationID, ErrCommPrefsAlreadySet
		}
		predicate = current.Predicate // flag is false; commit only if unchanged
	}

	// write the data first so a failure here leaves the flag unset and retryable.
	if err := m.writeMetadata(ctx, commPrefsData(commPrefs)); err != nil {
		return m.installationID, err
	}

	// flip comm_prefs_set last as the commit point; only one caller wins.
	err = m.store.SetIf(ctx, []byte(model.PartitionKey), flagKey, []byte(strconv.FormatBool(true)), predicate)
	if errors.Is(err, kv.ErrPredicateFailed) {
		// another writer committed comm_prefs_set concurrently
		m.commPrefsSet.Store(true)
		return m.installationID, ErrCommPrefsAlreadySet
	}
	if err != nil {
		return m.installationID, err
	}
	m.commPrefsSet.Store(true)
	return m.installationID, nil
}

// UpdateCommPrefs - updates the comm prefs metadata.
// When commPrefs is nil, we assume the setup is done and the user didn't provide any comm prefs.
// The data can be provided later as the web UI verifies if the comm prefs are set.
func (m *KVMetadataManager) UpdateCommPrefs(ctx context.Context, commPrefs *CommPrefs) (string, error) {
	meta := map[string]string{
		CommPrefsSetKeyName: strconv.FormatBool(commPrefs != nil),
	}
	if commPrefs != nil {
		// if commPrefs is not nil, we assume the setup is done and the user provided comm prefs
		for k, v := range commPrefsData(commPrefs) {
			meta[k] = v
		}
	}
	err := m.writeMetadata(ctx, meta)
	if err != nil {
		return m.installationID, err
	}

	// keep the cache in sync with what was written: a nil commPrefs writes
	// comm_prefs_set=false and must un-stick any previously cached true (e.g. a
	// Setup retry after an earlier write primed the cache).
	m.commPrefsSet.Store(commPrefs != nil)
	return m.installationID, nil
}

func (m *KVMetadataManager) IsInitialized(ctx context.Context) (bool, error) {
	if m.initialized.Load() {
		return true, nil
	}

	setupTimestamp, err := m.getSetupTimestamp(ctx)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	initialized := !setupTimestamp.IsZero()
	if initialized {
		m.initialized.Store(true)
	}
	return initialized, nil
}

// DockeEnvExists For testing purposes
var DockeEnvExists = "/.dockerenv"

func inK8sMetadata() string {
	_, k8s := os.LookupEnv("KUBERNETES_SERVICE_HOST")
	return strconv.FormatBool(k8s)
}

func inDockerMetadata() string {
	var err error
	if DockeEnvExists != "" {
		_, err = os.Stat(DockeEnvExists)
	}
	return strconv.FormatBool(err == nil)
}

func getInstrumentationMetadata() string {
	lakefsAccessKeyID := viper.GetString("installation.access_key_id")
	switch {
	case strings.HasSuffix(lakefsAccessKeyID, "LKFSSAMPLES"):
		return InstrumentationSamplesRepo
	case strings.HasSuffix(lakefsAccessKeyID, "QUICKSTART"):
		return InstrumentationQuickstart
	default:
		return InstrumentationRun
	}
}

func (m *KVMetadataManager) GetMetadata(ctx context.Context) (map[string]string, error) {
	metadata := make(map[string]string)
	metadata["lakefs_version"] = m.version
	metadata["lakefs_kv_type"] = m.kvType
	metadata["golang_version"] = runtime.Version()
	metadata["architecture"] = runtime.GOARCH
	metadata["os"] = runtime.GOOS
	metadata["is_k8s"] = inK8sMetadata()
	metadata["is_docker"] = inDockerMetadata()
	metadata["instrumentation"] = getInstrumentationMetadata()

	err := m.writeMetadata(ctx, metadata)
	if err != nil {
		return nil, err
	}
	// write installation id
	m.installationID, err = m.insertOrGetInstallationID(ctx, m.installationID)
	if err == nil {
		metadata["installation_id"] = m.installationID
	}

	setupTS, err := m.getSetupTimestamp(ctx)
	if err == nil {
		metadata[SetupTimestampKeyName] = setupTS.UTC().Format(time.RFC3339)
	}

	return metadata, nil
}
