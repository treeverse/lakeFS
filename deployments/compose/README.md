# lakeFS `docker-compose`

Compose file for lakeFS using variety of services lakeFS can work with.
Automatically creates a minIO bucket and lakeFS repository as part of the start up.

To spin up the environment, run:
```sh
docker compose up -d
```

That's it! To check the status of the containers:
```sh
docker compose ps
```

To access the lakeFS UI, go to http://localhost:8000. 

The docker-compose also runs the setup stage and configures admin user with the following credentials:
* Access key ID: `AKIAIOSFODNN7EXAMPLE`
* Secret access key: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` 


#### How to override env vars values 

To avoid undesired secrets leak into the public repository, it is recommended to override environment variables values by
creating a [.env](https://docs.docker.com/compose/environment-variables/#the-env-file) file under `/deployments/compose`.

### Hive Client

Can access Hive Server using client, run under 'client' profile:

```sh
docker compose --profile client run --rm hive-client
```

### Trino Client

Can access Trino using trino-cli, run under 'client' profile:

```sh
docker compose --profile client run --rm trino-client
```

