package dynamodb

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/cenkalti/backoff/v4"
	"github.com/treeverse/lakefs/pkg/kv"
	kvparams "github.com/treeverse/lakefs/pkg/kv/params"
)

type Driver struct{}

type Store struct {
	svc    *dynamodb.DynamoDB
	params *kvparams.DynamoDB
}

type EntriesIterator struct {
	scanCtx      context.Context
	entry        *kv.Entry
	err          error
	store        *Store
	queryResult  *dynamodb.QueryOutput
	currEntryIdx int
	partKey      []byte
	startKey     []byte
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

	DefaultDynamoDBTableName = "kvstore"
	// TODO (niro): Which values to use for DynamoDB tables?
	DefaultDynamoDBReadCapacityUnits  = 1000
	DefaultDynamoDBWriteCapacityUnits = 1000
)

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

func normalizeDBParams(p *kvparams.DynamoDB) {
	if len(p.TableName) == 0 {
		p.TableName = DefaultDynamoDBTableName
	}

	if p.ReadCapacityUnits == 0 {
		p.ReadCapacityUnits = DefaultDynamoDBReadCapacityUnits
	}

	if p.WriteCapacityUnits == 0 {
		p.WriteCapacityUnits = DefaultDynamoDBWriteCapacityUnits
	}
}

// Open - opens and returns a KV store over DynamoDB. This function creates the DB session
// and sets up the KV table.
func (d *Driver) Open(ctx context.Context, kvParams kvparams.KV) (kv.Store, error) {
	params := kvParams.DynamoDB
	if params == nil {
		return nil, fmt.Errorf("missing %s settings: %w", DriverName, kv.ErrDriverConfiguration)
	}

	normalizeDBParams(params)
	sess, err := session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
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
	// Create DynamoDB client
	svc := dynamodb.New(sess, cfg)
	err = setupKeyValueDatabase(ctx, svc, params)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrSetupFailed, err)
	}

	return &Store{
		svc:    svc,
		params: params,
	}, nil
}

// setupKeyValueDatabase setup everything required to enable kv over postgres
func setupKeyValueDatabase(ctx context.Context, svc *dynamodb.DynamoDB, params *kvparams.DynamoDB) error {
	// main kv table
	table, err := svc.CreateTableWithContext(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(params.TableName),
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
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(params.ReadCapacityUnits),
			WriteCapacityUnits: aws.Int64(params.WriteCapacityUnits),
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

	start := time.Now()
	result, err := s.svc.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName:              aws.String(s.params.TableName),
		Key:                    s.bytesKeyToDynamoKey(partitionKey, key),
		ConsistentRead:         aws.Bool(true),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	})
	const operation = "GetItem"
	dynamoRequestDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	if err != nil {
		dynamoFailures.WithLabelValues(operation).Inc()
		return nil, fmt.Errorf("get item: %s (key=%v): %w", err, string(key), kv.ErrOperationFailed)
	}
	if result.ConsumedCapacity != nil {
		dynamoConsumedCapacity.WithLabelValues(operation).Add(*result.ConsumedCapacity.CapacityUnits)
	}

	if result.Item == nil {
		return nil, fmt.Errorf("key=%v: %w", string(key), kv.ErrNotFound)
	}

	var item DynKVItem
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		return nil, fmt.Errorf("unmarshal map: %s (key=%v): %w", err, string(key), kv.ErrOperationFailed)
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
		return fmt.Errorf("marshal map: %s (key=%v): %w", err, string(key), kv.ErrOperationFailed)
	}

	input := &dynamodb.PutItemInput{
		Item:                   marshaledItem,
		TableName:              &s.params.TableName,
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	if usePredicate {
		if valuePredicate != nil {
			input.ConditionExpression = aws.String(ItemValue + " = :predicate")
			input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
				":predicate": {B: valuePredicate.([]byte)},
			}
		} else {
			input.ConditionExpression = aws.String("attribute_not_exists(" + ItemValue + ")")
		}
	}

	start := time.Now()
	resp, err := s.svc.PutItemWithContext(ctx, input)
	const operation = "PutItem"
	dynamoRequestDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	if err != nil {
		if _, ok := err.(*dynamodb.ConditionalCheckFailedException); ok && usePredicate {
			return kv.ErrPredicateFailed
		}
		dynamoFailures.WithLabelValues(operation).Inc()
		return fmt.Errorf("put item: %s (key=%v): %w", err, string(key), kv.ErrOperationFailed)
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

	start := time.Now()
	resp, err := s.svc.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName:              aws.String(s.params.TableName),
		Key:                    s.bytesKeyToDynamoKey(partitionKey, key),
		ReturnConsumedCapacity: aws.String(dynamodb.ReturnConsumedCapacityTotal),
	})
	const operation = "DeleteItem"
	dynamoRequestDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	if err != nil {
		dynamoFailures.WithLabelValues(operation).Inc()
		return fmt.Errorf("delete item: %s (key=%v): %w", err, string(key), kv.ErrOperationFailed)
	}
	if resp.ConsumedCapacity != nil {
		dynamoConsumedCapacity.WithLabelValues(operation).Add(*resp.ConsumedCapacity.CapacityUnits)
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, partitionKey, start []byte) (kv.EntriesIterator, error) {
	internalIter, err := s.scanInternal(ctx, partitionKey, start, nil)
	if err != nil {
		return nil, err
	}
	return internalIter, nil
}

func (s *Store) scanInternal(ctx context.Context, partitionKey, scanKey []byte, exclusiveStartKey map[string]*dynamodb.AttributeValue) (*EntriesIterator, error) {
	if len(partitionKey) == 0 {
		return nil, kv.ErrMissingPartitionKey
	}
	keyConditionExpression := PartitionKey + " = :partitionkey"
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":partitionkey": {
			B: partitionKey,
		},
	}
	if len(scanKey) > 0 {
		keyConditionExpression += " AND " + ItemKey + " >= :fromkey"
		expressionAttributeValues[":fromkey"] = &dynamodb.AttributeValue{
			B: scanKey,
		}
	}
	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(s.params.TableName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ConsistentRead:            aws.Bool(true),
		ScanIndexForward:          aws.Bool(true),
		ExclusiveStartKey:         exclusiveStartKey,
		ReturnConsumedCapacity:    aws.String(dynamodb.ReturnConsumedCapacityTotal),
	}
	if s.params.ScanLimit != 0 {
		queryInput.SetLimit(s.params.ScanLimit)
	}

	start := time.Now()
	queryOutput, err := s.svc.QueryWithContext(ctx, queryInput)
	const operation = "Query"
	dynamoRequestDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	if err != nil {
		dynamoFailures.WithLabelValues(operation).Inc()
		return nil, fmt.Errorf("query: %s (start=%v): %w ", err, string(scanKey), kv.ErrOperationFailed)
	}
	dynamoConsumedCapacity.WithLabelValues(operation).Add(*queryOutput.ConsumedCapacity.CapacityUnits)

	return &EntriesIterator{
		scanCtx:      ctx,
		store:        s,
		partKey:      partitionKey,
		startKey:     scanKey,
		queryResult:  queryOutput,
		currEntryIdx: 0,
		err:          nil,
	}, nil
}

func (s *Store) Close() {}

// DropTable used internally for testing purposes
func (s *Store) DropTable() error {
	_, err := s.svc.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: &s.params.TableName,
	})
	return err
}

func (e *EntriesIterator) Next() bool {
	if e.err != nil {
		return false
	}

	for e.currEntryIdx == int(*e.queryResult.Count) {
		if e.queryResult.LastEvaluatedKey == nil {
			return false
		}
		tmpEntriesIter, err := e.store.scanInternal(e.scanCtx, e.partKey, e.startKey, e.queryResult.LastEvaluatedKey)
		if err != nil {
			e.err = fmt.Errorf("scan paging: %w", err)
			return false
		}
		e.queryResult = tmpEntriesIter.queryResult
		e.currEntryIdx = 0
	}

	var item DynKVItem
	err := dynamodbattribute.UnmarshalMap(e.queryResult.Items[e.currEntryIdx], &item)
	if err != nil {
		e.err = fmt.Errorf("unmarshal map: %s: %w", err, kv.ErrOperationFailed)
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
