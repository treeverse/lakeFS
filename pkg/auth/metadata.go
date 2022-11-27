package auth

import (
	"context"
	b64 "encoding/base64"
	"errors"
	"runtime"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/treeverse/lakefs/pkg/auth/model"
	"github.com/treeverse/lakefs/pkg/kv"
)

const (
	InstallationIDKeyName 		= "installation_id"
	SetupTimestampKeyName 		= "setup_timestamp"
	CommPrefsSetKeyName			= "comm_prefs_set"
	EmailKeyName			  	= "hashed_user_email"
	FeatureUpdatesKeyName		= "feature_updates"
	SecurityUpdatesKeyName		= "security_updates"
)

type SetupStateName string
const (
	SetupStateInitialized 		SetupStateName   = "initialized"
	SetupStateNotInitialized 	SetupStateName = "not_initialized"
	SetupStateCommPrefsDone 	SetupStateName  = "comm_prefs_done"
)

//nolint:gochecknoinits
func init() {
	kv.MustRegisterType("auth", InstallationIDKeyName, nil)
	kv.MustRegisterType("auth", SetupTimestampKeyName, nil)
}

type MetadataManager interface {
	IsInitialized(ctx context.Context) (bool, error)
	GetSetupState(ctx context.Context, emailSubscriptionEnabled bool) (SetupStateName, error)
	UpdateCommPrefs(ctx context.Context, commPrefs CommPrefs) (string, error)
	UpdateSkipCommPrefs(ctx context.Context) error
	UpdateSetupTimestamp(context.Context, time.Time) error
	Write(context.Context) (map[string]string, error)
}

type KVMetadataManager struct {
	version        string
	kvType         string
	installationID string
	store          kv.Store
}

type CommPrefs struct {
	UserEmail string
	FeatureUpdates	bool
	SecurityUpdates bool
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
		UserEmail: string(email.Value),
		FeatureUpdates: hasFeatureUpdates,
		SecurityUpdates: hasSecurityUpdates,
	}, nil
}

func (m *KVMetadataManager) areCommPrefsSet(ctx context.Context) (bool, error) {
	commPrefsSet, err := m.store.Get(ctx, []byte(model.PartitionKey), []byte(model.MetadataKeyPath(CommPrefsSetKeyName)))
	if err != nil {
		return false, err
	}

	commPrefsSetBool, err := strconv.ParseBool(string(commPrefsSet.Value))
	if err != nil {
		return false, err
	}

	return commPrefsSetBool, nil
}

func (m *KVMetadataManager) GetSetupState(ctx context.Context, emailSubscriptionEnabled bool) (SetupStateName, error) {
	// for backwards compatibility (i.e. so existing instances don't need to go through setup again)
	// we first check if the setup timestamp has already been set
	isInitialized, err := m.IsInitialized(ctx)
	if err != nil {
		return "", err
	}

	if isInitialized {
		return SetupStateInitialized, nil
	}

	// if this feature is disabled, skip this step
	if emailSubscriptionEnabled {
		commPrefsSet, err := m.areCommPrefsSet(ctx)
		if err != nil || !commPrefsSet {
			return SetupStateNotInitialized, nil
		}
	}

	return SetupStateCommPrefsDone, nil
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

func (m *KVMetadataManager) UpdateSetupTimestamp(ctx context.Context, ts time.Time) error {
	return m.writeMetadata(ctx, map[string]string{
		SetupTimestampKeyName: ts.UTC().Format(time.RFC3339),
	})
}

func (m *KVMetadataManager) UpdateCommPrefs(ctx context.Context, commPrefs CommPrefs) (string, error) {
	encodedEmail := ""
	var err error
	if commPrefs.UserEmail != "" {
		encodedEmail = b64.StdEncoding.EncodeToString([]byte(commPrefs.UserEmail))
		if err != nil {
			return "", err
		}
	}
	
	return m.installationID, m.writeMetadata(ctx, map[string]string{
		EmailKeyName: encodedEmail,
		FeatureUpdatesKeyName: strconv.FormatBool(commPrefs.FeatureUpdates),
		SecurityUpdatesKeyName: strconv.FormatBool(commPrefs.SecurityUpdates),
		CommPrefsSetKeyName: strconv.FormatBool(true),
	})
}

func (m *KVMetadataManager) UpdateSkipCommPrefs(ctx context.Context) error {
	return m.writeMetadata(ctx, map[string]string{
		CommPrefsSetKeyName: strconv.FormatBool(true),
	})
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

func (m *KVMetadataManager) Write(ctx context.Context) (map[string]string, error) {
	metadata := make(map[string]string)
	metadata["lakefs_version"] = m.version
	metadata["lakefs_kv_type"] = m.kvType
	metadata["golang_version"] = runtime.Version()
	metadata["architecture"] = runtime.GOARCH
	metadata["os"] = runtime.GOOS
	err := m.writeMetadata(ctx, metadata)
	if err != nil {
		return nil, err
	}
	// write installation id
	m.installationID, err = m.insertOrGetInstallationID(ctx, m.installationID)
	if err == nil {
		metadata[InstallationIDKeyName] = m.installationID
	}

	setupTS, err := m.getSetupTimestamp(ctx)
	if err == nil {
		metadata[SetupTimestampKeyName] = setupTS.UTC().Format(time.RFC3339)
	}

	return metadata, err
}
