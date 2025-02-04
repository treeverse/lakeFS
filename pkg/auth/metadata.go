package auth

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"runtime"
	"strconv"
	"strings"
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
	FeatureUpdatesKeyName  = "feature_updates"
	SecurityUpdatesKeyName = "security_updates"

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
	IsCommPrefsSet(ctx context.Context) (bool, error)
	UpdateSetupTimestamp(ctx context.Context, setupTime time.Time, authType string) error
	GetMetadata(context.Context) (map[string]string, error)
}

type KVMetadataManager struct {
	version        string
	kvType         string
	installationID string
	store          kv.Store
}

type CommPrefs struct {
	UserEmail       string
	FeatureUpdates  bool
	SecurityUpdates bool
	InstallationID  string
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

func (m *KVMetadataManager) GetCommPrefs(ctx context.Context) (CommPrefs, error) {
	email, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(EmailKeyName)))
	if err != nil {
		return CommPrefs{}, err
	}
	featureUpdates, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(FeatureUpdatesKeyName)))
	if err != nil {
		return CommPrefs{}, err
	}
	securityUpdates, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(SecurityUpdatesKeyName)))
	if err != nil {
		return CommPrefs{}, err
	}

	hasFeatureUpdates, err := strconv.ParseBool(string(featureUpdates.Value))
	if err != nil {
		return CommPrefs{}, err
	}
	hasSecurityUpdates, err := strconv.ParseBool(string(securityUpdates.Value))
	if err != nil {
		return CommPrefs{}, err
	}

	return CommPrefs{
		UserEmail:       string(email.Value),
		FeatureUpdates:  hasFeatureUpdates,
		SecurityUpdates: hasSecurityUpdates,
	}, nil
}

func (m *KVMetadataManager) IsCommPrefsSet(ctx context.Context) (bool, error) {
	commPrefsSet, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(CommPrefsSetKeyName)))
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return false, ErrNotFound
		}
		return false, err
	}

	return strconv.ParseBool(string(commPrefsSet.Value))
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
	return m.writeMetadata(ctx, items)
}

// UpdateCommPrefs - updates the comm prefs metadata.
// When commPrefs is nil, we assume the setup is done and the user didn't provide any comm prefs.
// The data can be provided later as the web UI verifies if the comm prefs are set.
func (m *KVMetadataManager) UpdateCommPrefs(ctx context.Context, commPrefs *CommPrefs) (string, error) {
	var meta map[string]string
	if commPrefs != nil {
		// if commPrefs is not nil, we assume the setup is done and the user provided comm prefs
		meta = map[string]string{
			EmailKeyName:           base64.StdEncoding.EncodeToString([]byte(commPrefs.UserEmail)),
			FeatureUpdatesKeyName:  strconv.FormatBool(commPrefs.FeatureUpdates),
			SecurityUpdatesKeyName: strconv.FormatBool(commPrefs.SecurityUpdates),
			CommPrefsSetKeyName:    strconv.FormatBool(true),
		}
	} else {
		// if commPrefs is nil, we assume the setup is done and the user didn't provide any comm prefs
		meta = map[string]string{
			CommPrefsSetKeyName: strconv.FormatBool(false),
		}
	}
	err := m.writeMetadata(ctx, meta)
	return m.installationID, err
}

func (m *KVMetadataManager) IsInitialized(ctx context.Context) (bool, error) {
	setupTimestamp, err := m.getSetupTimestamp(ctx)
	if err != nil {
		if errors.Is(err, kv.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return !setupTimestamp.IsZero(), nil
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

	return metadata, err
}
