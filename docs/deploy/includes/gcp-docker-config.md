```yaml
database:
  connection_string: "[DATABASE_CONNECTION_STRING]"
auth:
  encrypt:
    secret_key: "[ENCRYPTION_SECRET_KEY]"
blockstore:
  type: gs
# Uncomment the following lines to give lakeFS access to your buckets using a service account:
# gs:
#   credentials_json: [YOUR SERVICE ACCOUNT JSON STRING]
```
