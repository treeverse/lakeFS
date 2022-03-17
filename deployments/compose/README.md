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

### DBT

Running DBT on lakeFS is available via Trino adapter or Spark adapter
Both adapters are configured in the dbt properties file (`dbt/profiles.yml`)
There is a sample dbt project under `dbt/dbt-project` 

### Using the Spark adapter (default)

The Spark adapter requires a Spark thrift server ( `spark-thrift` docker).
The Spark adapter is configured in the `profiles.yaml`.
In order to use the Spark adapter set `profile: 'spark'` in the `dbt_project.yaml` file.

### Using the Trino adapter 

The Trino adapter connects directly to the Trino server and is configured in the `profiles.yaml`,
In order to use the Trino adapter set `profile: 'trino'` in the `dbt_project.yaml` file.


### creating the DBT schema for main

The default schema configured in th DBT container (`properties.yaml`) is `dbt_main`.
To create the `dbt_main` schema run:
```bash
docker-compose run create-dbt-schema-main
```

### Running DBT commands

Run DBT commands using `docker-compose run dbt`.
The DBT commands run on the DBT project in `dbt/dbt-project`.

You could start by checking the environment:
```bash
docker-compose run dbt debug
```

Run your dbt project with:
```bash
docker-compose run dbt run
```

You could now see the generated objects in lakeFS (under path `lakefs://example/main/dbt/my_first_dbt_model/`) and query table `dbt_main.my_first_dbt_model` using the trino-client.

### Notebook

Jupyter notebook is the 'notebook' service.
It serves on port 8888.
The login password is set to 'lakefs'.

