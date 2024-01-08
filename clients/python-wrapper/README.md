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

## Examples

### Print sizes of all objects in lakefs://repo/main~2

```py
ref = lakefs.Repository("repo").ref("main~2")
for obj in ref.objects():
  print(f"{o.path}: {o.size_bytes}")
```

### Difference between two branches

```py
for i in lakefs.Repository("repo").ref("main").diff("twig"):
   print(i)
```

You can also use the [ref expression][lakefs-spec-ref]s here, for instance
`.diff("main~2")` also works.  Ref expressions are the lakeFS analogues of
[how Git specifies revisions][git-spec-rev].

### Search a stored object for a string

```py
with lakefs.Repository("repo").ref("main").object("path/to/data").reader(mode="r") as f:
   for l in f:
     if "quick" in l:
	   print(l)
```

### Upload and commit some data

```py
with lakefs.Repository("golden").branch("main").object("path/to/new").writer(mode="wb") as f:
   f.write(b"my data")

# Returns a Reference
lakefs.Repository("golden").branch("main").commit("added my data using lakeFS high-level SDK")

# Prints "my data"
with lakefs.Repository("golden").branch("main").object("path/to/new").reader(mode="r") as f:
   for l in f:
     print(l)
```

Unlike references, branches are readable.  This example couldn't work if we used a ref.

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

[lakeFS Python SDK](https://pydocs-lakefs.lakefs.io/) 

## Author

services@treeverse.io

[git-spec-rev]:  https://git-scm.com/docs/git-rev-parse#_specifying_revisions
[lakefs-spec-ref]:  https://docs.lakefs.io/understand/model.html#ref-expressions
