# azure-helm-values

```yaml
secrets:
    # replace this with the connection string of the database you created in a previous step:
    databaseConnectionString: [DATABASE_CONNECTION_STRING]
    # replace this with a randomly-generated string
    authEncryptSecretKey: [ENCRYPTION_SECRET_KEY]
lakefsConfig: |
    blockstore:
      type: azure
      azure:
        auth_method: msi # msi for active directory, access-key for access key 
     #  In case you chose to authenticate via access key unmark the following rows and insert the values from the previous step 
     #  storage_account: [your storage account]
     #  storage_access_key: [your access key]
    gateways:
      s3:
        # replace this with the host you will use for the lakeFS S3-compatible endpoint:
        domain_name: s3.lakefs.example.com
```

