package dynamodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Driver struct{}

type Store struct {
	svc    *dynamodb.Client
	params *kvparams.DynamoDB
	wg     sync.WaitGroup
	logger logging.Logger
	cancel chan bool
}

type EntriesIterator struct {
	partitionKey      []byte
	startKey          []byte
	exclusiveStartKey map[string]types.AttributeValue

	scanCtx      context.Context
	entry        *kv.Entry
	err          error
	store        *Store
	queryResult  *dynamodb.QueryOutput
	currEntryIdx int
	limit        int64
}

type DynKVItem struct {
	PartitionKey []byte
	ItemKey      []byte
	ItemValue    []byte
}

const (
	DriverName = "dynamodb"

	PartitionKey = "PartitionKey"
	ItemKey      = "ItemKey"
	ItemValue    = "ItemValue"
)

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

// Open - opens and returns a KV store over DynamoDB. This function creates the DB session
// and sets up the KV table.
func (d *Driver) Open(ctx context.Context, kvParams kvparams.Config) (kv.Store, error) {
	params := kvParams.DynamoDB
	if params == nil {
		return nil, fmt.Errorf("missing %s settings: %w", DriverName, kv.ErrDriverConfiguration)
	}

	var opts []func(*config.LoadOptions) error
	if params.AwsRegion != "" {
		opts = append(opts, config.WithRegion(params.AwsRegion))
	}
	if params.AwsProfile != "" {
		opts = append(opts, config.WithSharedConfigProfile(params.AwsProfile))
	}
	if params.AwsAccessKeyID != "" {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			params.AwsAccessKeyID,
			params.AwsSecretAccessKey,
			"",
		)))
	}
	const maxConnectionPerHost = 10
	opts = append(opts, config.WithHTTPClient(
		awshttp.NewBuildableClient().WithTransportOptions(func(transport *http.Transport) {
			transport.MaxConnsPerHost = maxConnectionPerHost
		})))

	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}

	// Create DynamoDB client
	svc := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if params.Endpoint != "" {
			o.BaseEndpoint = &params.Endpoint
		}
	})

	err = setupKeyValueDatabase(ctx, svc, params)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrSetupFailed, err)
	}

	logger := logging.FromContext(ctx).WithField("store", DriverName)
	s := &Store{
		svc:    svc,
		params: params,
		logger: logger,
		cancel: make(chan bool),
	}

	s.StartPeriodicCheck()
	return s, nil
}

// isTableExist will try to describeTable and return bool status, error is returned only in case err != ResourceNotFoundException
func isTableExist(ctx context.Context, svc *dynamodb.Client, table string) (bool, error) {
	_, err := svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		var errResNotFound *types.ResourceNotFoundException
		if errors.As(err, &errResNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// setupKeyValueDatabase setup everything required to enable kv over postgres
func setupKeyValueDatabase(ctx context.Context, svc *dynamodb.Client, params *kvparams.DynamoDB) error {
	// main kv table
	exist, err := isTableExist(ctx, svc, params.TableName)
	if exist || err != nil {
		return err
	}

	table, err := svc.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(params.TableName),
		BillingMode: types.BillingModePayPerRequest, // On-Demand
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(PartitionKey),
				AttributeType: types.ScalarAttributeTypeB,
			},
			{
				AttributeName: aws.String(ItemKey),
				AttributeType: types.ScalarAttributeTypeB,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(PartitionKey),
				KeyType:       types.KeyTypeHash,
			},
			{
				AttributeName: aws.String(ItemKey),
				KeyType:       types.KeyTypeRange,
			},
		},
	})
	if err != nil {
		var errResInUse *types.ResourceInUseException
		if errors.As(err, &errResInUse) {
			return nil
		}
		return err
	}
	bo := backoff.NewExponentialBackOff()
	const (
		maxInterval = 5
		maxElapsed  = 30
	)

	bo.MaxInterval = maxInterval * time.Second
	bo.MaxElapsedTime = maxElapsed * time.Second
	err = backoff.Retry(func() error {
		desc, err := svc.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: table.TableDescription.TableName,
		})
		if err != nil {
			// we shouldn't retry on anything but kv.ErrTableNotActive
			return backoff.Permanent(err)
		}
		if desc.Table.TableStatus != types.TableStatusActive {
			return fmt.Errorf("table status(%s): %w", desc.Table.TableStatus, kv.ErrTableNotActive)
		}
		return nil
	}, bo)
	return err
}

func (s *Store) bytesKeyToDynamoKey(partitionKey, key []byte) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		PartitionKey: &types.AttributeValueMemberB{
			Value: partitionKey,
		},
		ItemKey: &types.AttributeValueMemberB{
			Value: key,
		},
	}
}

func (s *Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return nil, kv.ErrMissingKey
	}
	result, err := s.svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:              aws.String(s.params.TableName),
		Key:                    s.bytesKeyToDynamoKey(partitionKey, key),
		ConsistentRead:         aws.Bool(true),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	const operation = "GetItem"
	if err != nil {
		return nil, fmt.Errorf("get item: %w", err)
	}
	if result.ConsumedCapacity != nil {
		dynamoConsumedCapacity.WithLabelValues(operation).Add(*result.ConsumedCapacity.CapacityUnits)
	}

	if result.Item == nil {
		return nil, kv.ErrNotFound
	}

	var item DynKVItem
	err = attributevalue.UnmarshalMap(result.Item, &item)
	if err != nil {
		return nil, fmt.Errorf("unmarshal map: %w", err)
	}

	return &kv.ValueWithPredicate{
		Value:     item.ItemValue,
		Predicate: kv.Predicate(item.ItemValue),
	}, nil
}

func (s *Store) Set(ctx context.Context, partitionKey, key, value []byte) error {
	return s.setWithOptionalPredicate(ctx, partitionKey, key, value, nil, false)
}

func (s *Store) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	return s.setWithOptionalPredicate(ctx, partitionKey, key, value, valuePredicate, true)
}

func (s *Store) setWithOptionalPredicate(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate, usePredicate bool) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}

	item := DynKVItem{
		PartitionKey: partitionKey,
		ItemKey:      key,
		ItemValue:    value,
	}

	marshaledItem, err := attributevalue.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal map: %w", err)
	}

	input := &dynamodb.PutItemInput{
		Item:                   marshaledItem,
		TableName:              &s.params.TableName,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}
	if usePredicate {
		switch valuePredicate {
		case nil: // Set only if not exists
			input.ConditionExpression = aws.String("attribute_not_exists(" + ItemValue + ")")

		case kv.PrecondConditionalExists: // update only if exists
			input.ConditionExpression = aws.String("attribute_exists(" + ItemValue + ")")

		default: // update only if predicate matches the current stored value
			predicateCondition := expression.Name(ItemValue).Equal(expression.Value(valuePredicate.([]byte)))
			conditionExpression, err := expression.NewBuilder().WithCondition(predicateCondition).Build()
			if err != nil {
				return fmt.Errorf("build condition expression: %w", err)
			}
			input.ExpressionAttributeNames = conditionExpression.Names()
			input.ExpressionAttributeValues = conditionExpression.Values()
			input.ConditionExpression = conditionExpression.Condition()
		}
	}

	resp, err := s.svc.PutItem(ctx, input)
	const operation = "PutItem"
	if err != nil {
		var errConditionalCheckFailed *types.ConditionalCheckFailedException
		if usePredicate && errors.As(err, &errConditionalCheckFailed) {
			return kv.ErrPredicateFailed
		}
		return fmt.Errorf("put item: %w", err)
	}
	if resp.ConsumedCapacity != nil {
		dynamoConsumedCapacity.WithLabelValues(operation).Add(*resp.ConsumedCapacity.CapacityUnits)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, partitionKey, key []byte) error {
	if len(partitionKey) == 0 {
		return kv.ErrMissingPartitionKey
	}
	if len(key) == 0 {
		return kv.ErrMissingKey
	}

	resp, err := s.svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName:              aws.String(s.params.TableName),
		Key:                    s.bytesKeyToDynamoKey(partitionKey, key),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	const operation = "DeleteItem"
	if err != nil {
		return fmt.Errorf("delete item: %w", err)
	}
	if resp.ConsumedCapacity != nil {
		dynamoConsumedCapacity.WithLabelValues(operation).Add(*resp.ConsumedCapacity.CapacityUnits)
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, partitionKey []byte, options kv.ScanOptions) (kv.EntriesIterator, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	// limit set to the minimum 'params.ScanLimit' and 'options.BatchSize', unless 0 (not set)
	limit := s.params.ScanLimit
	batchSize := int64(options.BatchSize)
	if batchSize != 0 && limit != 0 && batchSize < limit {
		limit = batchSize
	}
	it := &EntriesIterator{
		partitionKey: partitionKey,
		startKey:     options.KeyStart,
		scanCtx:      ctx,
		store:        s,
		limit:        limit,
	}
	it.runQuery()
	if it.err != nil {
		return nil, it.err
	}
	return it, nil
}

func (s *Store) Close() {
	s.StopPeriodicCheck()
}

// DropTable used internally for testing purposes
func (s *Store) DropTable() error {
	ctx := context.Background()
	_, err := s.svc.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: &s.params.TableName,
	})
	return err
}

func (e *EntriesIterator) SeekGE(key []byte) {
	if !e.isInRange(key) {
		e.startKey = key
		e.exclusiveStartKey = nil
		e.runQuery()
		return
	}
	var item DynKVItem
	e.currEntryIdx = sort.Search(len(e.queryResult.Items), func(i int) bool {
		if e.err = attributevalue.UnmarshalMap(e.queryResult.Items[i], &item); e.err != nil {
			return false
		}
		return bytes.Compare(key, item.ItemKey) <= 0
	})
}

func (e *EntriesIterator) Next() bool {
	if e.err != nil {
		return false
	}
	for e.currEntryIdx == len(e.queryResult.Items) {
		if e.queryResult.LastEvaluatedKey == nil {
			return false
		}
		e.exclusiveStartKey = e.queryResult.LastEvaluatedKey
		e.runQuery()
		if e.err != nil {
			return false
		}
	}
	var item DynKVItem
	e.err = attributevalue.UnmarshalMap(e.queryResult.Items[e.currEntryIdx], &item)
	if e.err != nil {
		return false
	}
	e.entry = &kv.Entry{
		Key:   item.ItemKey,
		Value: item.ItemValue,
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

func (e *EntriesIterator) runQuery() {
	expressionAttributeValues := map[string]types.AttributeValue{
		":partitionkey": &types.AttributeValueMemberB{
			Value: e.partitionKey,
		},
	}
	keyConditionExpression := PartitionKey + " = :partitionkey"
	if len(e.startKey) > 0 {
		keyConditionExpression += " AND " + ItemKey + " >= :fromkey"
		expressionAttributeValues[":fromkey"] = &types.AttributeValueMemberB{
			Value: e.startKey,
		}
	}
	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(e.store.params.TableName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ConsistentRead:            aws.Bool(true),
		ScanIndexForward:          aws.Bool(true),
		ExclusiveStartKey:         e.exclusiveStartKey,
		ReturnConsumedCapacity:    types.ReturnConsumedCapacityTotal,
	}
	if e.limit != 0 {
		queryInput.Limit = aws.Int32(int32(e.limit))
	}

	queryResult, err := e.store.svc.Query(e.scanCtx, queryInput)
	const operation = "Query"
	if err != nil {
		e.err = fmt.Errorf("query: %w", err)
		return
	}
	dynamoConsumedCapacity.WithLabelValues(operation).Add(*queryResult.ConsumedCapacity.CapacityUnits)
	e.queryResult = queryResult
	e.currEntryIdx = 0
}

func (e *EntriesIterator) isInRange(key []byte) bool {
	if len(e.queryResult.Items) == 0 {
		return false
	}
	var maxItem, minItem DynKVItem
	e.err = attributevalue.UnmarshalMap(e.queryResult.Items[0], &minItem)
	if e.err != nil {
		return false
	}
	e.err = attributevalue.UnmarshalMap(e.queryResult.Items[len(e.queryResult.Items)-1], &maxItem)
	if e.err != nil {
		return false
	}
	return bytes.Compare(key, minItem.ItemKey) >= 0 && bytes.Compare(key, maxItem.ItemKey) <= 0
}

// StartPeriodicCheck performs one check and continues every 'interval' in the background
func (s *Store) StartPeriodicCheck() {
	interval := s.params.HealthCheckInterval
	if interval <= 0 {
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.logger.WithField("interval", interval).Debug("Starting DynamoDB health check")
		// check first and loop for checking every interval
		s.Check()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.Check()
			case <-s.cancel:
				return
			}
		}
	}()
}

func (s *Store) Check() {
	log := s.logger.WithField("store_type", DriverName)
	success, err := isTableExist(context.Background(), s.svc, s.params.TableName)
	if success {
		log.Debug("DynamoDB health check passed!")
	} else {
		log.WithError(err).Debug("DynamoDB health check failed")
	}
}

func (s *Store) StopPeriodicCheck() {
	if s.cancel != nil {
		close(s.cancel)
		s.wg.Wait()
		s.cancel = nil
	}
}
