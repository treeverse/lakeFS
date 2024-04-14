# Python SDK System Tests

This package consists of the integration tests for the python SDK library.
Please make sure to add / modify tests whenever pushing new code to the SDK.

These tests can be run against a lakeFS instance with the following environment configuration:

## Export lakectl environment variables

```sh
export LAKECTL_CREDENTIALS_ACCESS_KEY_ID=<your_lakefs_access_key_id>
export LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=<your_lakefs_secret_access_key>
export LAKECTL_SERVER_ENDPOINT_URL=<your_lakefs_endpoint>
```

## Export test environment variables

```sh
export STORAGE_NAMESPACE=<base storage namespace for your tests>
export TAG=<lakeFS server version (dev)>
```

## Create a venv and install dependencies

From the clients/python-wrapper directory:

```sh
python3 -m venv <your_venv_path>
source <your_venv_path>/bin/activate
pip install -r requirements
```

## Run Tests

```sh
pytest tests
```
