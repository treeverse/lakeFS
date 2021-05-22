```yaml
secrets:
    # replace DATABASE_CONNECTION_STRING with the connection string of the database you created in a previous step.
    # e.g. postgres://postgres:myPassword@my-lakefs-db.rds.amazonaws.com:5432/lakefs
    databaseConnectionString: [DATABASE_CONNECTION_STRING]
    # replace this with a randomly-generated string
    authEncryptSecretKey: [ENCRYPTION_SECRET_KEY]
lakefsConfig: |
    blockstore:
      type: s3
      s3:
        region: us-east-1
    gateways:
      s3:
        # replace this with the host you will use for the lakeFS S3-compatible endpoint:
        domain_name: [S3_GATEWAY_DOMAIN]
```
