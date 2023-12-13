# lakeFS High-Level Python SDK

lakeFS High Level SDK for Python, provides developers with the following features:
1. Simpler programming interface with less configuration
2. Inferring identity from environment 
3. Better abstractions for common, more complex operations (I/O, transactions, imports)

## Requirements

Python 3.9+

## Installation & Usage

### pip install

```sh
pip install lakefs
```

### Import the package

```python
import lakefs
```

## Getting Started

Please follow the [installation procedure](#installation--usage) and afterward refer to the following example snippet for a quick start:

```python

import lakefs
from lakefs.client import Client

# Using default client will attempt to authenticate with lakeFS server using configured credentials
# If environment variables or .lakectl.yaml file exist 
repo = lakefs.repository(repository_id="my-repo")

# Or explicitly initialize and provide a Client object 
clt = Client(username="<lakefs_access_key_id>", password="<lakefs_secret_access_key>", host="<lakefs_endpoint>")
repo = lakefs.Repository(repository_id="my-repo", client=clt)

# From this point, proceed using the package according to documentation
main_branch = repo.create(storage_namespace="<storage_namespace>").branch(branch_id="main")
...
```

## Tests

To run the tests using `pytest`, first clone the lakeFS git repository

```sh
git clone https://github.com/treeverse/lakeFS.git
cd lakefs/clients/python-wrapper
```

### Unit Tests

Inside the `tests` folder, execute `pytest utests` to run the unit tests.

### Integration Tests

See [testing documentation](https://github.com/treeverse/lakeFS/blob/master/clients/python-wrapper/tests/integration/README.md) for more information

## Documentation

[lakeFS Python SDK](http://pydocs-lakefs.lakefs.io/) 

## Author

services@treeverse.io


