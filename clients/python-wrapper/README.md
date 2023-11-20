# lakefs-sdk-wrapper (Pre-release Alpha)

This is a wrapper package for the lakefs-sdk API, providing the following features:
1. Provide a simpler programming interface with less configuration 
2. Behave closer to other related Python SDKs
3. Allow inferring identity from environment 
4. Provide better abstractions for common, more complex operations (I/O, transactions, imports)

## Requirements.

Python 3.9+

## Installation & Usage
### pip install

```sh
pip install lakefs
```

Then import the package:
```python
import lakefs
```

### Tests

To run the tests using `pytest`, first clone the lakeFS git repository

```sh
git clone https://github.com/treeverse/lakeFS.git
```

#### Unit Tests

Inside the `tests` folder, execute `pytest utests` to run the unit tests.

#### Integration Tests

See [documentation](https://github.com/treeverse/lakeFS/blob/master/clients/python-wrapper/tests/integration/README.md) for more information

## Getting Started

Please follow the [installation procedure](#installation--usage) and then run the following:

```python

import lakefs
from lakefs.client import Client, init

# Using default client will attempt to authenticate with lakeFS server using configured credentials
# If environment variables or .lakectl.yaml file exist 
repo = lakefs.Repository(repository_id="my-repo")

# Alternatively, provide explicit credentials and call client init method, which will initialize the default client
init(username="<my_username>", password="<my_password>", host="<lakefs_host>")
repo = lakefs.Repository(repository_id="my-repo")

# Or explicitly initialize and provide a Client object 
clt = Client(username="<my_username>", password="<my_password>", host="<lakefs_host>")
repo = lakefs.Repository(repository_id="my-repo", client=clt)

# From this point, proceed using the package according to documentation
main_branch = repo.create(storage_namespace="<storage_namespace>").branch(branch_id="main")
...
```

## Documentation

[//]: # (Add link to documentation site)
[index](https://pypi.org/project/lakefs/) 

## Author

services@treeverse.io


