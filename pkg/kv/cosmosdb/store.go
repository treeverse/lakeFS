package cosmosdb

import (
	"context"
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
	"net/http"
)

type Driver struct{}

type Store struct {
	containerClient  *azcosmos.ContainerClient
	consistencyLevel azcosmos.ConsistencyLevel
}

const (
	DriverName = "cosmosdb"
)

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

	var client *azcosmos.Client
	if params.ReadWriteKey != "" {
		cred, err := azcosmos.NewKeyCredential(params.ReadWriteKey)
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
		client, err = azcosmos.NewClient("myAccountEndpointURL", cred, nil)
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
	}, nil
}

func getOrCreateDatabase(ctx context.Context, client *azcosmos.Client, params *kvparams.CosmosDB) (*azcosmos.DatabaseClient, error) {
	dbClient, err := client.NewDatabase(params.Database)
	if err != nil {
		return nil, fmt.Errorf("creating database client: %w", err)
	}
	dbResp, err := dbClient.Read(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("reading database: %w", err)
	}
	switch dbResp.RawResponse.StatusCode {
	case http.StatusOK:
		return nil, nil
	case http.StatusNotFound:
		tp := azcosmos.NewAutoscaleThroughputProperties(400)
		dbResp, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: params.Database},
			&azcosmos.CreateDatabaseOptions{
				ThroughputProperties: &tp})
		if err != nil || dbResp.RawResponse.StatusCode != http.StatusCreated {
			return nil, fmt.Errorf("reading database(%d): %w", dbResp.RawResponse.StatusCode, err)
		}
	default:
		return nil, fmt.Errorf("reading database(%d): %w", dbResp.RawResponse.StatusCode, err)
	}
	return dbClient, nil
}

func getOrCreateContainer(ctx context.Context, dbClient *azcosmos.DatabaseClient, params *kvparams.CosmosDB) (*azcosmos.ContainerClient, error) {
	containerClient, err := dbClient.NewContainer(params.Container)
	if err != nil {
		return nil, fmt.Errorf("creating database client: %w", err)
	}
	cResp, err := containerClient.Read(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("reading database: %w", err)
	}
	switch cResp.RawResponse.StatusCode {
	case http.StatusOK:
		return nil, nil
	case http.StatusNotFound:
		tp := azcosmos.NewAutoscaleThroughputProperties(400)
		cResp, err = dbClient.CreateContainer(ctx,
			azcosmos.ContainerProperties{
				ID: params.Container,
				PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
					Paths: []string{"/partitionKey"},
				},
			}, &azcosmos.CreateContainerOptions{
				ThroughputProperties: &tp,
			})

		if err != nil || cResp.RawResponse.StatusCode != http.StatusCreated {
			return nil, fmt.Errorf("creating container(%d): %w", cResp.RawResponse.StatusCode, err)
		}
	default:
		return nil, fmt.Errorf("reading database(%d): %w", cResp.RawResponse.StatusCode, err)
	}

	containerClient.
	return containerClient, nil
}

// encoding is the encoding used to encode the partition keys, ids and values.
// Must be an encoding that keeps the strings in-order.
var encoding = base32.HexEncoding // Encoding that keeps the strings in-order.

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
		PartitionKey: encoding.EncodeToString(partitionKey),
		ID:           encoding.EncodeToString(key),
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

func isErrStatusCode(err error, code int) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) && respErr.StatusCode == code {
		return true
	}
	return false
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
		ID:           encoding.EncodeToString(key),
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
		ID:           encoding.EncodeToString(key),
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

	_, err := s.containerClient.DeleteItem(ctx, pk, encoding.EncodeToString(key), nil)
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

	queryPager := s.containerClient.NewQueryItemsPager("select * from c where c.id >= @start order by c.id", pk, &azcosmos.QueryOptions{
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
			e.err = err
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
	key, err := e.encoding.DecodeString(itemResponseBody.ID)
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
