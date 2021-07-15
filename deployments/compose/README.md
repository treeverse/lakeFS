# lakeFS docker-compsoe

Compose file for lakeFS using variety of services lakeFS can work with.


### Hive Client

Can access Hive Server using client, run under 'client' profile:

```sh
docker-compose --profile client run --rm hive-client
```

### Trino Client

Can access Trino using trino-cli, run under 'client' profile:

```sh
docker-compose --profile client run --rm trino-client
```

