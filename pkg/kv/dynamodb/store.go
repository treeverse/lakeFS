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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/kv"
	"github.com/treeverse/lakefs/pkg/kv/kvparams"
	"github.com/treeverse/lakefs/pkg/logging"
)

type Driver struct{}

type Store struct {
	svc    *dynamodb.DynamoDB
	params *kvparams.DynamoDB
	wg     sync.WaitGroup
	logger logging.Logger
	cancel chan bool
}

type EntriesIterator struct {
	partitionKey      []byte
	startKey          []byte
	exclusiveStartKey map[string]*dynamodb.AttributeValue

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

	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Profile:           params.AwsProfile,
	})
	if err != nil {
		return nil, err
	}

	cfg := aws.NewConfig()
	if params.Endpoint != "" {
		cfg.Endpoint = aws.String(params.Endpoint)
	}
	if params.AwsRegion != "" {
		cfg = cfg.WithRegion(params.AwsRegion)
	}
	if params.AwsAccessKeyID != "" {
		cfg = cfg.WithCredentials(credentials.NewCredentials(
			&credentials.StaticProvider{
				Value: credentials.Value{
					AccessKeyID:     params.AwsAccessKeyID,
					SecretAccessKey: params.AwsSecretAccessKey,
				},
			}))
	}
	const maxConnectionPerHost = 10
	cfg = cfg.WithHTTPClient(&http.Client{
		Transport: &http.Transport{
			MaxConnsPerHost: maxConnectionPerHost,
		},
	})

	// Create DynamoDB client
	svc := dynamodb.New(sess, cfg)
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
func isTableExist(ctx context.Context, svc *dynamodb.DynamoDB, table string) (bool, error) {
	_, err := svc.DescribeTableWithContext(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table),
	})
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return false, nil
		}
		return false, handleClientError(err)
	}
	return true, nil
}

// setupKeyValueDatabase setup everything required to enable kv over postgres
func setupKeyValueDatabase(ctx context.Context, svc *dynamodb.DynamoDB, params *kvparams.DynamoDB) error {
	// main kv table
	exist, err := isTableExist(ctx, svc, params.TableName)
	if exist || err != nil {
		return err
	}

	table, err := svc.CreateTableWithContext(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(params.TableName),
		BillingMode: aws.String(dynamodb.BillingModePayPerRequest), // On-Demand
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(PartitionKey),
				AttributeType: aws.String("B"),
			},
			{
				AttributeName: aws.String(ItemKey),
				AttributeType: aws.String("B"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(PartitionKey),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String(ItemKey),
				KeyType:       aws.String("RANGE"),
			},
		},
	})
	if err != nil {
		if _, ok := err.(*dynamodb.ResourceInUseException); ok {
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
		desc, err := svc.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: table.TableDescription.TableName,
		})
		if err != nil {
			// we shouldn't retry on anything but kv.ErrTableNotActive
			return backoff.Permanent(err)
		}
		if *desc.Table.TableStatus != dynamodb.TableStatusActive {
			return fmt.Errorf("table status(%s): %w", *desc.Table.TableStatus, kv.ErrTableNotActive)
		}
		return nil
	}, bo)
	return err
}

func (s *Store) bytesKeyToDynamoKey(partitionKey, key []byte) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		PartitionKey: {
			B: partitionKey,
		},
		ItemKey: {
			B: key,
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

	result, err := s.svc.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:              aws.String(s.params.TableName),
		Key:                    s.bytesKeyToDynamoKey(partitionKey, key),
		ConsistentRead:         aws.Bool(true),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	})
	const operation = "GetItem"
	if err != nil {
		return nil, fmt.Errorf("get item: %w", handleClientError(err))
	}
	if result.ConsumedCapacity != nil {
		dynamoConsumedCapacity.WithLabelValues(operation).Add(*result.ConsumedCapacity.CapacityUnits)
	}

	if result.Item == nil {
		return nil, kv.ErrNotFound
	}

	var item DynKVItem
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
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

	marshaledItem, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("marshal map: %w", err)
	}

	input := &dynamodb.PutItemInput{
		Item:                   marshaledItem,
		TableName:              &s.params.TableName,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	if usePredicate {
		switch valuePredicate {
		case nil: // Set only if not exists
			input.ConditionExpression = aws.String("attribute_not_exists(" + ItemValue + ")")

		case kv.PrecondConditionalExists: // update only if exists
			input.ConditionExpression = aws.String("attribute_exists(" + ItemValue + ")")

		default: // update only if predicate matches current stored value
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

	resp, err := s.svc.PutItemWithContext(ctx, input)
	const operation = "PutItem"
	if err != nil {
		if _, ok := err.(*dynamodb.ConditionalCheckFailedException); ok && usePredicate {
			return kv.ErrPredicateFailed
		}
		return fmt.Errorf("put item: %w", handleClientError(err))
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

	resp, err := s.svc.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName:              aws.String(s.params.TableName),
		Key:                    s.bytesKeyToDynamoKey(partitionKey, key),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	})
	const operation = "DeleteItem"
	if err != nil {
		return fmt.Errorf("delete item: %w", handleClientError(err))
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
	_, err := s.svc.DeleteTable(&dynamodb.DeleteTableInput{
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
		if e.err = dynamodbattribute.UnmarshalMap(e.queryResult.Items[i], &item); e.err != nil {
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
	e.err = dynamodbattribute.UnmarshalMap(e.queryResult.Items[e.currEntryIdx], &item)
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
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":partitionkey": {
			B: e.partitionKey,
		},
	}
	keyConditionExpression := PartitionKey + " = :partitionkey"
	if len(e.startKey) > 0 {
		keyConditionExpression += " AND " + ItemKey + " >= :fromkey"
		expressionAttributeValues[":fromkey"] = &dynamodb.AttributeValue{
			B: e.startKey,
		}
	}
	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(e.store.params.TableName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ConsistentRead:            aws.Bool(true),
		ScanIndexForward:          aws.Bool(true),
		ExclusiveStartKey:         e.exclusiveStartKey,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	if e.limit != 0 {
		queryInput.SetLimit(e.limit)
	}

	queryResult, err := e.store.svc.QueryWithContext(e.scanCtx, queryInput)
	const operation = "Query"
	if err != nil {
		e.err = fmt.Errorf("query: %w", handleClientError(err))
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
	e.err = dynamodbattribute.UnmarshalMap(e.queryResult.Items[0], &minItem)
	if e.err != nil {
		return false
	}
	e.err = dynamodbattribute.UnmarshalMap(e.queryResult.Items[len(e.queryResult.Items)-1], &maxItem)
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

func handleClientError(err error) error {
	// extract original error if needed
	var reqErr awserr.Error
	if errors.As(err, &reqErr) && errors.Is(reqErr.OrigErr(), context.Canceled) {
		err = reqErr.OrigErr()
	}
	return err
}
