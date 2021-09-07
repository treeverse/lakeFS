```yaml
database:
  connection_string: "postgres://user:pass@<AZURE_POSTGRES_SERVER_NAME>..."
auth:
  encrypt:
    secret_key: "<RANDOM_GENERATED_STRING>"
blockstore:
  type: azure
  azure:
    auth_method: msi # msi for active directory, access-key for access key 
    #  In case you chose to authenticate via access key replace unmark the following rows and insert the values from the previous step 
    #  storage_account: <your storage account>
    #  storage_access_key: <your access key>
```
