package cosmosdb

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/treeverse/lakefs/pkg/ident"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Driver struct{}

type Store struct {
	containerClient  *azcosmos.ContainerClient
	consistencyLevel azcosmos.ConsistencyLevel
	logger           logging.Logger
}

const (
	DriverName = "cosmosdb"
)

// encoding is the encoding used to encode the partition keys, ids and values.
// Must be an encoding that keeps the strings in-order.
var encoding = base32.HexEncoding // Encoding that keeps the strings in-order.

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

// Open - opens and returns a KV store over CosmosDB. This function creates the DB session
// and sets up the KV table.
func (d *Driver) Open(ctx context.Context, kvParams kvparams.Config) (kv.Store, error) {
	params := kvParams.CosmosDB
	if params == nil {
		return nil, fmt.Errorf("missing %s settings: %w", DriverName, kv.ErrDriverConfiguration)
	}
	if params.Endpoint == "" {
		return nil, fmt.Errorf("missing endpoint: %w", kv.ErrDriverConfiguration)
	}
	if params.Database == "" {
		return nil, fmt.Errorf("missing database: %w", kv.ErrDriverConfiguration)
	}
	if params.Container == "" {
		return nil, fmt.Errorf("missing container: %w", kv.ErrDriverConfiguration)
	}

	logger := logging.FromContext(ctx).WithField("store", DriverName)
	logger.Infof("CosmosDB: connecting to %s", params.Endpoint)

	var client *azcosmos.Client
	if params.Key != "" {
		cred, err := azcosmos.NewKeyCredential(params.Key)
		if err != nil {
			return nil, fmt.Errorf("creating key: %w", err)
		}

		// hook for using emulator for testing
		if params.Client == nil {
			params.Client = http.DefaultClient
		}
		// Create a CosmosDB client
		client, err = azcosmos.NewClientWithKey(params.Endpoint, cred, &azcosmos.ClientOptions{
			ClientOptions: azcore.ClientOptions{
				Transport: params.Client,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("creating client using access key: %w", err)
		}
	} else {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("default creds: %w", err)
		}
		client, err = azcosmos.NewClient(params.Endpoint, cred, nil)
		if err != nil {
			return nil, fmt.Errorf("creating client with default creds: %w", err)
		}
	}

	dbClient, err := getOrCreateDatabase(ctx, client, params)
	if err != nil {
		return nil, err
	}

	// Create container client
	containerClient, err := getOrCreateContainer(ctx, dbClient, params)
	if err != nil {
		return nil, err
	}

	cLevel := azcosmos.ConsistencyLevelBoundedStaleness
	if !params.StrongConsistency {
		cLevel = azcosmos.ConsistencyLevelSession
	}
	return &Store{
		containerClient:  containerClient,
		consistencyLevel: cLevel,
		logger:           logger,
	}, nil
}

func getOrCreateDatabase(ctx context.Context, client *azcosmos.Client, params *kvparams.CosmosDB) (*azcosmos.DatabaseClient, error) {
	_, err := client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: params.Database}, nil)
	if err != nil {
		if errStatusCode(err) != http.StatusConflict {
			return nil, fmt.Errorf("creating database: %w", err)
		}
	}
	dbClient, err := client.NewDatabase(params.Database)
	if err != nil {
		return nil, fmt.Errorf("init database client: %w", err)
	}
	return dbClient, nil
}

func getOrCreateContainer(ctx context.Context, dbClient *azcosmos.DatabaseClient, params *kvparams.CosmosDB) (*azcosmos.ContainerClient, error) {
	_, err := dbClient.CreateContainer(ctx,
		azcosmos.ContainerProperties{
			ID: params.Container,
			PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
				Paths: []string{"/partitionKey"},
			},
			// Excluding the value field from indexing since it is not used in queries and saves RUs for writes.
			// partitionKey is automatically not indexed. The rest of the fields are indexed by default, including id
			// which is unnecessary, but cannot be excluded.
			IndexingPolicy: &azcosmos.IndexingPolicy{
				Automatic:     false,
				IndexingMode:  azcosmos.IndexingModeConsistent,
				IncludedPaths: []azcosmos.IncludedPath{{Path: "/*"}},
				ExcludedPaths: []azcosmos.ExcludedPath{{Path: "/value/?"}},
			},
		}, nil)
	if err != nil {
		if errStatusCode(err) != http.StatusConflict {
			return nil, fmt.Errorf("creating container: %w", err)
		}
	}
	containerClient, err := dbClient.NewContainer(params.Container)
	if err != nil {
		return nil, fmt.Errorf("init container client: %w", err)
	}
	return containerClient, nil
}

// hashID returns a hash of the key that is used as the document id.
func (s *Store) hashID(key []byte) string {
	return encoding.EncodeToString(ident.NewAddressWriter().MarshalBytes(key).Identity())
}

type Document struct {
	PartitionKey string `json:"partitionKey"`
	// ID is the hash of the key. It is used as the document id for lookup of a single item.
	// CosmosDB has a 1023 byte limit on the id, so we hash the key to ensure it fits.
	ID string `json:"id"`
	// Key is the original key. It is not used listing of items by order.
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return nil, kv.ErrMissingKey
	}
	item := Document{
		PartitionKey: encoding.EncodeToString(partitionKey),
		ID:           s.hashID(key),
	}
	pk := azcosmos.NewPartitionKeyString(item.PartitionKey)

	// Read an item
	itemResponse, err := s.containerClient.ReadItem(ctx, pk, item.ID, nil)
	if err != nil {
		if isErrStatusCode(err, http.StatusNotFound) {
			return nil, kv.ErrNotFound
		}
		return nil, err
	}

	var itemResponseBody Document
	err = json.Unmarshal(itemResponse.Value, &itemResponseBody)
	if err != nil {
		return nil, err
	}

	val, err := encoding.DecodeString(itemResponseBody.Value)
	if err != nil {
		return nil, err
	}
	return &kv.ValueWithPredicate{
		Value:     val,
		Predicate: kv.Predicate([]byte(itemResponse.ETag)),
	}, nil
}

func errStatusCode(err error) int {
	var respErr *azcore.ResponseError
	if !errors.As(err, &respErr) {
		return -1
	}
	return respErr.StatusCode
}

func isErrStatusCode(err error, code int) bool {
	return errStatusCode(err) == code
}

func (s *Store) Set(ctx context.Context, partitionKey, key, value []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}

	// Specifies the value of the partiton key
	item := Document{
		PartitionKey: encoding.EncodeToString(partitionKey),
		ID:           s.hashID(key),
		Key:          encoding.EncodeToString(key),
		Value:        encoding.EncodeToString(value),
	}

	b, err := json.Marshal(item)
	if err != nil {
		return err
	}
	itemOptions := azcosmos.ItemOptions{
		ConsistencyLevel: s.consistencyLevel.ToPtr(),
	}
	pk := azcosmos.NewPartitionKeyString(item.PartitionKey)

	_, err = s.containerClient.UpsertItem(ctx, pk, b, &itemOptions)
	return err
}

func (s *Store) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}

	// Specifies the value of the partiton key
	item := Document{
		PartitionKey: encoding.EncodeToString(partitionKey),
		ID:           s.hashID(key),
		Key:          encoding.EncodeToString(key),
		Value:        encoding.EncodeToString(value),
	}

	b, err := json.Marshal(item)
	if err != nil {
		return err
	}
	itemOptions := azcosmos.ItemOptions{
		ConsistencyLevel: s.consistencyLevel.ToPtr(),
	}
	pk := azcosmos.NewPartitionKeyString(item.PartitionKey)

	switch valuePredicate {
	case nil:
		_, err = s.containerClient.CreateItem(ctx, pk, b, &itemOptions)
		if isErrStatusCode(err, http.StatusConflict) {
			return kv.ErrPredicateFailed
		}
	case kv.PrecondConditionalExists:
		patch := azcosmos.PatchOperations{}
		patch.AppendReplace("/value", item.Value)
		_, err = s.containerClient.PatchItem(
			ctx,
			pk,
			item.ID,
			patch,
			&itemOptions,
		)
		if isErrStatusCode(err, http.StatusNotFound) {
			return kv.ErrPredicateFailed
		}
	default:
		etag := azcore.ETag(valuePredicate.([]byte))
		itemOptions.IfMatchEtag = &etag
		_, err = s.containerClient.UpsertItem(ctx, pk, b, &itemOptions)
		if isErrStatusCode(err, http.StatusPreconditionFailed) {
			return kv.ErrPredicateFailed
		}
	}
	return err
}

func (s *Store) Delete(ctx context.Context, partitionKey, key []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	pk := azcosmos.NewPartitionKeyString(encoding.EncodeToString(partitionKey))

	_, err := s.containerClient.DeleteItem(ctx, pk, s.hashID(key), nil)
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode != http.StatusNotFound {
		return err
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}

	pk := azcosmos.NewPartitionKeyString(encoding.EncodeToString(partitionKey))

	queryPager := s.containerClient.NewQueryItemsPager("select * from c where c.key >= @start order by c.key", pk, &azcosmos.QueryOptions{
		ConsistencyLevel: s.consistencyLevel.ToPtr(),
		PageSizeHint:     int32(options.BatchSize),
		QueryParameters: []azcosmos.QueryParameter{{
			Name:  "@start",
			Value: encoding.EncodeToString(options.KeyStart),
		}},
	})
	currPage, err := queryPager.NextPage(ctx)
	if err != nil {
		return nil, err
	}

	return &EntriesIterator{
		queryPager: queryPager,
		currPage:   currPage,
		queryCtx:   ctx,
		encoding:   encoding,
	}, nil
}

func (s *Store) Close() {
}

type EntriesIterator struct {
	entry        *kv.Entry
	err          error
	currEntryIdx int
	queryPager   *runtime.Pager[azcosmos.QueryItemsResponse]
	queryCtx     context.Context
	currPage     azcosmos.QueryItemsResponse
	encoding     *base32.Encoding
}

func (e *EntriesIterator) Next() bool {
	if e.err != nil {
		return false
	}

	if e.currEntryIdx >= len(e.currPage.Items) {
		if !e.queryPager.More() {
			return false
		}
		var err error
		e.currPage, err = e.queryPager.NextPage(e.queryCtx)
		if err != nil {
			e.err = fmt.Errorf("getting next page: %w", err)
			return false
		}
		e.currEntryIdx = 0
	}

	var itemResponseBody Document
	err := json.Unmarshal(e.currPage.Items[e.currEntryIdx], &itemResponseBody)
	if err != nil {
		e.err = fmt.Errorf("failed to unmarshal: %w", err)
		return false
	}
	key, err := e.encoding.DecodeString(itemResponseBody.Key)
	if err != nil {
		e.err = fmt.Errorf("failed to decode id: %w", err)
		return false
	}
	value, err := e.encoding.DecodeString(itemResponseBody.Value)
	if err != nil {
		e.err = fmt.Errorf("failed to decode value: %w", err)
		return false
	}

	e.entry = &kv.Entry{
		Key:   key,
		Value: value,
	}

	e.currEntryIdx++
	return true
}

func (e *EntriesIterator) Entry() *kv.Entry {
	return e.entry
}

func (e *EntriesIterator) Err() error {
	return e.err
}

func (e *EntriesIterator) Close() {
	e.err = kv.ErrClosedEntries
}
