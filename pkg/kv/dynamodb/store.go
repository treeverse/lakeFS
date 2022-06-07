package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/treeverse/lakefs/pkg/kv"
)

type Driver struct{}

type Store struct {
	svc    *dynamodb.DynamoDB
	params *Params
}

type EntriesIterator struct {
	entry        *kv.Entry
	err          error
	store        *Store
	queryResult  *dynamodb.QueryOutput
	currEntryIdx int
	partKey      string
	startKey     string
}

type DynKVItem struct {
	PartitionKey string
	ItemKey      string
	ItemValue    string
}

type Params struct {
	TableName          string
	ReadCapacityUnits  int64
	WriteCapacityUnits int64

	Endpoint           string
	AwsRegion          string
	AwsAccessKeyID     string
	AwsSecretAccessKey string
}

const (
	DriverName       = "dynamodb"
	DefaultTableName = "kvstore"
	// TBD: Which values to use?
	DefaultReadCapacityUnits  = 1000
	DefaultWriteCapacityUnits = 1000

	PartitionKey = "PartitionKey"
	ItemKey      = "ItemKey"
	ItemValue    = "ItemValue"
)

//nolint:gochecknoinits
func init() {
	kv.Register(DriverName, &Driver{})
}

// Open - opens and returns a KV store over DynamoDB. This function creates the DB session
// and sets up the KV table. dsn is a string with the DynamoDB endpoint
func (d *Driver) Open(ctx context.Context, dsn string) (kv.Store, error) {
	// TODO: Get table name from env
	params, err := parseDsn(dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrDriverConfiguration, err)
	}

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	// Create DynamoDB client
	svc := dynamodb.New(sess,
		aws.NewConfig().
			WithEndpoint(params.Endpoint).
			WithRegion(params.AwsRegion).
			WithCredentials(credentials.NewCredentials(
				&credentials.StaticProvider{
					Value: credentials.Value{
						AccessKeyID:     params.AwsAccessKeyID,
						SecretAccessKey: params.AwsSecretAccessKey,
					}})))

	err = setupKeyValueDatabase(ctx, svc, params)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", kv.ErrSetupFailed, err)
	}

	return &Store{
		svc:    svc,
		params: params,
	}, nil
}

func parseDsn(dsn string) (*Params, error) {
	params := &Params{
		TableName:          DefaultTableName,
		ReadCapacityUnits:  DefaultReadCapacityUnits,
		WriteCapacityUnits: DefaultWriteCapacityUnits,
	}
	err := json.Unmarshal([]byte(dsn), params)
	if err != nil {
		return nil, err
	}
	return params, nil
}

// setupKeyValueDatabase setup everything required to enable kv over postgres
func setupKeyValueDatabase(ctx context.Context, svc *dynamodb.DynamoDB, params *Params) error {
	// main kv table
	_, err := svc.CreateTableWithContext(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(params.TableName),
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(PartitionKey),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String(ItemKey),
				AttributeType: aws.String("S"),
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
		if _, ok := err.(*dynamodb.ResourceInUseException); !ok {
			return err
		}
	}
	return nil
}

func (s *Store) bytesKeyToDynamoKey(partitionKey, key []byte) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		PartitionKey: {
			S: aws.String(string(partitionKey)),
		},
		ItemKey: {
			S: aws.String(string(key)),
		},
	}
}

func (s *Store) Get(ctx context.Context, partitionKey, key []byte) (*kv.ValueWithPredicate, error) {
	if len(key) == 0 {
		return nil, kv.ErrMissingKey
	}

	result, err := s.svc.GetItemWithContext(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.params.TableName),
		Key:       s.bytesKeyToDynamoKey(partitionKey, key),
	})
	if err != nil {
		return nil, err
	}

	if result.Item == nil {
		return nil, kv.ErrNotFound
	}

	var item DynKVItem
	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}

	return &kv.ValueWithPredicate{
		Value:     []byte(item.ItemValue),
		Predicate: kv.Predicate([]byte(item.ItemValue)),
	}, nil
}

func (s *Store) Set(ctx context.Context, partitionKey, key, value []byte) error {
	return s.setWithOptionalPredicate(ctx, partitionKey, key, value, nil, false)
}

func (s *Store) SetIf(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate) error {
	return s.setWithOptionalPredicate(ctx, partitionKey, key, value, valuePredicate, true)
}

func (s *Store) setWithOptionalPredicate(ctx context.Context, partitionKey, key, value []byte, valuePredicate kv.Predicate, usePredicate bool) error {
	if len(key) == 0 {
		return kv.ErrMissingKey
	}
	if value == nil {
		return kv.ErrMissingValue
	}

	item := DynKVItem{
		PartitionKey: string(partitionKey),
		ItemKey:      string(key),
		ItemValue:    string(value),
	}

	marshaledItem, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}

	input := &dynamodb.PutItemInput{
		Item:      marshaledItem,
		TableName: &s.params.TableName,
	}
	if usePredicate {
		if valuePredicate != nil {
			input.ConditionExpression = aws.String(ItemValue + " = :predicate")
			input.ExpressionAttributeValues = map[string]*dynamodb.AttributeValue{
				":predicate": {S: aws.String(string(valuePredicate.([]byte)))},
			}
		} else {
			input.ConditionExpression = aws.String("attribute_not_exists(" + ItemValue + ")")
		}
	}

	_, err = s.svc.PutItemWithContext(ctx, input)

	if err != nil {
		if _, ok := err.(*dynamodb.ConditionalCheckFailedException); ok && usePredicate {
			return kv.ErrPredicateFailed
		}
		return fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	return nil
}

func (s *Store) Delete(ctx context.Context, partitionKey, key []byte) error {
	if len(key) == 0 {
		return kv.ErrMissingKey
	}

	_, err := s.svc.DeleteItemWithContext(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.params.TableName),
		Key:       s.bytesKeyToDynamoKey(partitionKey, key),
	})

	if err != nil {
		return fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, partitionKey, start []byte) (kv.EntriesIterator, error) {
	return s.scanInternal(ctx, partitionKey, start, nil)
}

func (s *Store) scanInternal(ctx context.Context, partitionKey, scanKey []byte, exclusiveStartKey map[string]*dynamodb.AttributeValue) (*EntriesIterator, error) {
	keyConditionExpression := PartitionKey + " = :partitionkey"
	expressionAttributeValues := map[string]*dynamodb.AttributeValue{
		":partitionkey": {
			S: aws.String(string(partitionKey)),
		},
	}
	if len(scanKey) > 0 {
		keyConditionExpression += " AND " + ItemKey + " >= :fromkey"
		expressionAttributeValues[":fromkey"] = &dynamodb.AttributeValue{
			S: aws.String(string(scanKey)),
		}
	}
	queryInput := &dynamodb.QueryInput{
		TableName:                 aws.String(s.params.TableName),
		KeyConditionExpression:    aws.String(keyConditionExpression),
		ExpressionAttributeValues: expressionAttributeValues,
		ConsistentRead:            aws.Bool(true),
		ScanIndexForward:          aws.Bool(true),
		ExclusiveStartKey:         exclusiveStartKey,
	}
	queryOutput, err := s.svc.QueryWithContext(ctx, queryInput)
	if err != nil {
		return nil, fmt.Errorf("%s (start=%v): %w ", err, string(scanKey), kv.ErrOperationFailed)
	}

	return &EntriesIterator{
		store:        s,
		partKey:      string(partitionKey),
		startKey:     string(scanKey),
		queryResult:  queryOutput,
		currEntryIdx: 0,
		err:          nil,
	}, nil
}

func (s *Store) Close() {}

func (e *EntriesIterator) Next() bool {
	if e.err != nil {
		return false
	}

	if e.currEntryIdx == int(*e.queryResult.Count) {
		if e.queryResult.LastEvaluatedKey == nil {
			return false
		}
		tmpEntriesIter, err := e.store.scanInternal(context.Background(), []byte(e.partKey), []byte(e.startKey), e.queryResult.LastEvaluatedKey)
		if err != nil {
			e.err = fmt.Errorf("scanning table: %w", err)
			return false
		}
		e.queryResult = tmpEntriesIter.queryResult
		e.currEntryIdx = 0
	}

	var item DynKVItem
	err := dynamodbattribute.UnmarshalMap(e.queryResult.Items[e.currEntryIdx], &item)
	if err != nil {
		e.err = fmt.Errorf("%s: %w", err, kv.ErrOperationFailed)
	}
	e.entry = &kv.Entry{
		Key:   []byte(item.ItemKey),
		Value: []byte(item.ItemValue),
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
