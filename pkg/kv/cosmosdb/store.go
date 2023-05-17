package cosmosdb

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"log"
	"net/http"
)

type Driver struct{}

type Store struct {
	endpoint        string
	key             string
	databaseName    string
	containerName   string
	client          *azcosmos.Client
	databaseClient  *azcosmos.DatabaseClient
	containerClient *azcosmos.ContainerClient
}

const (
	DriverName = "cosmosdb"
)

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

// Open - opens and returns a KV store over DynamoDB. This function creates the DB session
// and sets up the KV table.
func (d *Driver) Open(ctx context.Context, kvParams kvparams.Config) (kv.Store, error) {
	params := kvParams.CosmosDB
	if params == nil {
		return nil, fmt.Errorf("missing %s settings: %w", DriverName, kv.ErrDriverConfiguration)
	}

	cred, err := azcosmos.NewKeyCredential(params.ReadWriteKey)
	if err != nil {
		return nil, fmt.Errorf("creating key: %w", err)
	}

	// Create a CosmosDB client
	client, err := azcosmos.NewClientWithKey(params.Endpoint, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}

	databaseClient, err := client.NewDatabase(params.Database)
	if err != nil {
		return nil, fmt.Errorf("creating database client: %w", err)
	}

	// Create container client
	containerClient, err := client.NewContainer(params.Database, params.Container)
	if err != nil {
		return nil, fmt.Errorf("creating container client: %w", err)
	}

	return &Store{
		client:          client,
		databaseClient:  databaseClient,
		containerClient: containerClient,
	}, nil
}

type Document struct {
	PartitionKey string `json:"partitionKey"`
	ID           string `json:"id"`
	Value        string `json:"value"`
}

func (s *Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return nil, kv.ErrMissingKey
	}
	item := Document{
		PartitionKey: base32.HexEncoding.EncodeToString(partitionKey),
		ID:           base32.HexEncoding.EncodeToString(key),
	}
	pk := azcosmos.NewPartitionKeyString(item.PartitionKey)

	// Read an item
	itemResponse, err := s.containerClient.ReadItem(ctx, pk, item.ID, nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return nil, kv.ErrNotFound
		}
		return nil, err
	}

	var itemResponseBody Document
	err = json.Unmarshal(itemResponse.Value, &itemResponseBody)
	if err != nil {
		return nil, err
	}

	val, err := base32.HexEncoding.DecodeString(itemResponseBody.Value)
	if err != nil {
		return nil, err
	}
	return &kv.ValueWithPredicate{
		Value:     val,
		Predicate: kv.Predicate([]byte(itemResponse.ETag)),
	}, nil
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
		PartitionKey: base32.HexEncoding.EncodeToString(partitionKey),
		ID:           base32.HexEncoding.EncodeToString(key),
		Value:        base32.HexEncoding.EncodeToString(value),
	}

	b, err := json.Marshal(item)
	if err != nil {
		return err
	}
	itemOptions := azcosmos.ItemOptions{
		ConsistencyLevel: azcosmos.ConsistencyLevelBoundedStaleness.ToPtr(),
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
		PartitionKey: base32.HexEncoding.EncodeToString(partitionKey),
		ID:           base32.HexEncoding.EncodeToString(key),
		Value:        base32.HexEncoding.EncodeToString(value),
	}

	b, err := json.Marshal(item)
	if err != nil {
		return err
	}
	itemOptions := azcosmos.ItemOptions{
		ConsistencyLevel: azcosmos.ConsistencyLevelBoundedStaleness.ToPtr(),
	}
	pk := azcosmos.NewPartitionKeyString(item.PartitionKey)

	if valuePredicate == nil {
		_, err = s.containerClient.CreateItem(ctx, pk, b, &itemOptions)
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusConflict {
			return kv.ErrPredicateFailed
		}
		return err
	} else if valuePredicate == kv.PrecondConditionalExists {
		patch := azcosmos.PatchOperations{}
		patch.AppendReplace("/value", item.Value)
		_, err = s.containerClient.PatchItem(
			ctx,
			pk,
			item.ID,
			patch,
			&itemOptions,
		)
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return kv.ErrPredicateFailed
		}
		return err
	}

	etag := azcore.ETag(valuePredicate.([]byte))
	itemOptions.IfMatchEtag = &etag
	_, err = s.containerClient.UpsertItem(ctx, pk, b, &itemOptions)
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == http.StatusPreconditionFailed {
		return kv.ErrPredicateFailed
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
	pk := azcosmos.NewPartitionKeyString(base32.HexEncoding.EncodeToString(partitionKey))

	_, err := s.containerClient.DeleteItem(ctx, pk, base32.HexEncoding.EncodeToString(key), nil)
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

	pk := azcosmos.NewPartitionKeyString(base32.HexEncoding.EncodeToString(partitionKey))

	queryPager := s.containerClient.NewQueryItemsPager("select * from c where c.id >= @start order by c.id ", pk, &azcosmos.QueryOptions{
		ConsistencyLevel: azcosmos.ConsistencyLevelBoundedStaleness.ToPtr(),
		PageSizeHint:     int32(options.BatchSize),
		QueryParameters: []azcosmos.QueryParameter{{
			Name:  "@start",
			Value: base32.HexEncoding.EncodeToString(options.KeyStart),
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
		limit:      int64(options.BatchSize),
		start:      options.KeyStart,
	}, nil
}

func (s *Store) Close() {
}

type EntriesIterator struct {
	entry                  *kv.Entry
	err                    error
	store                  *Store
	currEntryIdx           int
	keyConditionExpression string
	limit                  int64
	queryPager             *runtime.Pager[azcosmos.QueryItemsResponse]
	queryCtx               context.Context
	start                  []byte
	currPage               azcosmos.QueryItemsResponse
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
			e.err = err
			return false
		}
		e.currEntryIdx = 0
	}

	var itemResponseBody Document
	err := json.Unmarshal(e.currPage.Items[e.currEntryIdx], &itemResponseBody)
	if err != nil {
		log.Default().Fatalf("Failed to unmarshal: %v", err)
	}
	key, err := base32.HexEncoding.DecodeString(itemResponseBody.ID)
	if err != nil {
		log.Default().Fatalf("Failed to decode: %v", err)
	}
	value, err := base32.HexEncoding.DecodeString(itemResponseBody.Value)
	if err != nil {
		log.Default().Fatalf("Failed to decode: %v", err)
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
