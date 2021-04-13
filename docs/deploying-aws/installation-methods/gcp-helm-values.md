```yaml
secrets:
    # replace DATABASE_CONNECTION_STRING with the connection string of the database you created in a previous step.
    # e.g.: postgres://postgres:myPassword@localhost/postgres:5432
    databaseConnectionString: [DATABASE_CONNECTION_STRING]
    # replace this with a randomly-generated string
    authEncryptSecretKey: [ENCRYPTION_SECRET_KEY]
lakefsConfig: |
    blockstore:
      type: gs
    # Uncomment the following lines to give lakeFS access to your buckets using a service account:
    # gs:
    #   credentials_json: [YOUR SERVICE ACCOUNT JSON STRING]
    gateways:
      s3:
        # replace this with the host you will use for the lakeFS S3-compatible endpoint:
        domain_name: [S3_GATEWAY_DOMAIN]
```
   **Notes for running lakeFS on GKE**
   * To connect to your database, you need to use one of the ways of [connecting GKE to Cloud SQL](https://cloud.google.com/sql/docs/mysql/connect-kubernetes-engine#cloud-sql-auth-proxy-with-workload-identity).
   * To give lakeFS access to your bucket, you can start the cluster in [storage-rw](https://cloud.google.com/container-registry/docs/access-control#gke) mode. Alternatively, you can use a service account JSON string by uncommenting the `gs.credentials_json` property in the following yaml.
