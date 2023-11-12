# Python SDK Wrapper System Tests

This package consists of the system tests for the python SDK wrapper.
Please make sure to add / modify tests whenever pushing new code to the SDK wrapper.

These tests can be run against a lakeFS instance with the following environment configuration:

## Export lakectl environment variables
```
LAKECTL_CREDENTIALS_ACCESS_KEY_ID=<your_lakefs_access_key_id>
LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=<your_lakefs_secret_access_key>
LAKECTL_SERVER_ENDPOINT_URL=<your_lakefs_endpoint>
```

## Create a venv and install dependencies

From the clients/python-wrapper directory:
```
python3 -m venv <your_venv_path>
source <your_venv_path>/bin/activate
pip install -r requirements pylint
```

## Run Tests
```
pylint tests
```
