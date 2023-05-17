package cosmosdb_test

//var testParams *kvparams.DynamoDB
//
//func TestMain(m *testing.M) {
//	databaseURI, cleanupFunc, err := testutil.GetDynamoDBInstance()
//	if err != nil {
//		log.Fatalf("Could not connect to Docker: %s", err)
//	}
//
//	testParams = &kvparams.DynamoDB{
//		TableName:          testutil.UniqueKVTableName(),
//		ScanLimit:          10,
//		Endpoint:           databaseURI,
//		AwsRegion:          "us-east-1",
//		AwsAccessKeyID:     "fakeMyKeyId",
//		AwsSecretAccessKey: "fakeSecretAccessKey",
//	}
//
//	code := m.Run()
//	cleanupFunc()
//	os.Exit(code)
//}
