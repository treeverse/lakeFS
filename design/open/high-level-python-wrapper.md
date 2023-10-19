# lakefs high level Python SDK Wrapper

## Goals

1. Provide a simpler programming interface with less configuration
1. Behave closer to other related Python SDKs (Pinecone, OpenAI, HuggingFace, ...)
1. Allow infering identity from environment
1. Provide better abstractions for common, more complex operations (I/O, imports)

## Non-Goals

1. For now, we explicitly leave out environment administration: setting up server, configuring GC rules, all IAM/ACL related things.

## Authentication

Any operation that calls out to lakeFS will try to authenticate using the following chain:

1. All models receive an optional `client` kwarg with explicit credentials
1. Otherwise, if `lakefs.init(...)` is called with parameters (`access_key_id`, `jwt_token`, ...) - these will be set on a `lakefs.DefaultClient` object
1. If `lakefs.init()` is called with no parameters, or `init` is not called:
    1. use `LAKECTL_ACCESS_KEY_ID` and `LAKECTL_ACCESS_SECRET_KEY` if set
    1. Otherwise, use `~/.lakectl.yaml` if exists
    1. Otherwise, try and use IAM role from current machine (using AWS IAM role (will only work with enterprise/cloud))
1. If init is not called, it will be lazily called on the first use of `DefaultClient`, deferring authentication to the first API call.

## API wrapper interface

the higher lavel SDK will be resource-class based. performing API operations will be
done by calling the methods on their parent object. Examples:

```python
import lakefs

repo = lakefs.repository('example')
branch = repo.branch('main')
for item in branch.list_objects(prefix='foo/'):
    if item.path.endswith('.parquet'):
        print(item.path)

data: bytes = branch.download('datasets/foo/1.parquet')
branch.upload(data, 'foo/bar/baz.parquet')

# this will work:
data: bytes = lakefs.repository('example').commit('abc123').download('a/b.txt')

# since commits are immutable, this method will not exist:
lakefs.repository('example').commit('abc123').upload(data, 'a/b.txt')
```

## Partial interface definition

### Authentication

```python
class Client:
    """
    Wrapper around lakefs_sdk's client object
    Takes care of instantiating it from the environment
    """
    def __init__(self, **kwargs):
        self._client = _infer_auth_chain(**kwargs)

# global default client
DefaultClient: Optional[Client] = None

try:
    DefaultClient = Client()
except NoAuthenticationFound:
    # must call init() explicitly
    DefaultClient = None


def init(**kwargs):
    global DefaultClient
    DefaultClient = Client(**kwargs)

```

### model driven interface



```python
class Repository:
    def __init__(self, repository_id: str, client: Client = DefaultClient): ...
    def get_or_create(self, storage_namespace: str, default_branch_id: str = 'main', include_samples: bool = False) -> Repository: ...
    def branch(self, branch_id: str) -> Branch: ...
    def list_branches(self, max_amount: Optional[int], after: str ='', prefix: str ='') -> Generator[Branch]: ...
    def commit(self, commit_id: str) -> Reference: ...
    def tag(self, tag_id: str) -> Tag: ...
    def list_tags(self, max_amount: Optional[int], after: str ='', prefix: str ='') -> Generator[Tag]: ...
    def metadata(self) -> dict[str, str]: ...


class ReadIOBase:
    def download(self, path: str) -> bytes: ...
    def list_objects(self, max_amount: Optional[int], after: str ='', prefix: str ='', delimiter: str = '/') -> Generator[ObjectInfo | CommonPrefix]: ...
    def stat_object(self, path: str) -> ObjectInfo: ...
    def open(self, path, mode: Literal['r', 'rb'], pre_signed: Optional[bool] = None) -> TextIO | BinaryIO: ...


class Reference(ReadIOBase):
    def __init__(self, repository_id: str, reference_id: str, client: Client = DefaultClient): ...
    def log(self, max_amount: Optional[int]) -> Generator[Reference]: ...
    def metadata(self) -> dict[str, str]: ...
    def commit_message(self) -> str: ...
    def diff(self, other_ref: str | Reference, max_amount: Optional[int], after: str ='', prefix: str ='', delimiter: str = '/') -> Generator[Change]: ...
    def merge_into(self, destination_branch_id: str | Branch): ...


class WriteIOBase:
    def upload(self, data: bytes | str | TextIO | BinaryIO, path: str) -> ObjectInfo: ...
    def import_from(self, object_store_uri: str, destination: str, commit_message: str) -> ImportManager: ...
    def delete_objects(self, object_paths: str | Iterable[str]): ...
    def copy_object(self, from_reference: str, from_path: str): ...
    def open(self, path, mode: Literal['r', 'rb', 'w', 'wb'], pre_signed: Optional[bool] = None) -> TextIO | BinaryIO: ...


class Branch(Reference, WriteIOBase):
    def create(self, source_reference_id: str) -> Branch: ...
    def head(self) -> Reference: ...
    def commit_changes(self, message: str, metadata: dict[str, str]) -> Reference: ...
    def reset(self, path: Optional[str] = None): ...
    def reverse(self, reference_id: str): ...
    def list_uncommitted(self, max_amount: Optional[int], after: str ='', prefix: str ='') -> Generator[Change]: ...
    def delete(self): ...
    def transaction(self, commit_message: str) -> BranchTransaction: ...
    # override
    def open(self, path, mode: Literal['r', 'rb', 'w', 'wb'], pre_signed: Optional[bool] = None) -> TextIO | BinaryIO: ...


class Tag(Reference):
    def delete(self): ...


class CommonPrefix:
    def __init__(self, repository_id: str, reference_id: str, path: str, client: Client = DefaultClient): ...
    def path(self) -> self: ...
    def exists(self) -> bool: ...


class ObjectInfo:
    def __init__(self, repository_id: str, reference_id: str, path: str, client: Client = DefaultClient): ...
    def path(self) -> self: ...
    def modified_time(self) -> datetime.datetime: ...
    def size_bytes(self) -> int: ...
    def content_type(self) -> Optional[str]: ...
    def metadata(self) -> dict[str, str]: ...
    def physical_address(self) -> str: ...
    def delete(self): ...


class Change(NamedTuple):
    type: Literal(['added', 'removed', 'changed', 'conflict,', 'prefix_changed'])
    path: str
    path_type: Literal(['common_prefix', 'object'])
    size_bytes: Optional[int]


class ServerConfiguration:
    def __init__(self, client: Client = DefaultClient): ...
    def version(self) -> str: ...
    def storage_config(self) -> ServerStorageConfiguration: ...


class ServerStorageConfiguration:
    def __init__(self, client: Client = DefaultClient): ...
    def blockstore_type(self) -> str: ...
    def pre_sign_support(self) -> bool: ...
    def import_support(self) -> bool: ...


class ImportManager:
    def __init__(self, repository_id: str, reference_id: str, client: Client = DefaultClient): ...
    def start(self) -> str:
        'start import, reporting back (and storing) a process id'
        ...
    def wait(self, poll_interval: timedelta = timedelta(seconds=2)) -> ImportResult:
        'poll a started import task ID, blocking until completion'
        ...
    def run(self, poll_interval: Optional[timedelta] = None) -> ImportResult:
        'same as calling start() and then wait()'
        ...


class ImportResult(NamedTuple):
    commit: Commit
    ingested_objects: int

class BranchTransaction(WriteIOBase):
    def __init__(self, repository_id: str, source_branch_id: str, client: Client = DefaultClient): ...
    
    def __enter__(self):
        'Create ephemeral branch from the source branch (e.g. <source_branch_id>-txn-<uuid>'
        ...

    def __exit__(self, type, value, traceback):
        """if successful: commit, merge, delete ephemeral branch
           otherwise, leave branch and report with a meaningful error
        """
        ...

```

While this list is fairly exhaustive, it might require a few additional tweaks and additions.

## Higher Level Utilities

### I/O - reading/writing objects

Provide a pythonic `open()` method that returns a "file-like object", passable to other frameworks or libraries
that can deal with one.

```python
import lakefs

repo = lakefs.repository('example')
branch = repo.branch('main')

data: bytes = b'hello world\n'
with branch.open('foo/bar.txt', 'w') as f:
    # Will check the underlying client for pre-signed URL support
    #  if supported, will do the whole get_physical_address, http upload, link address dance
    #  Otherwise, direct upload.
    #  *In the future*, we can accept a stream/file-like object, sniff for its size/content type
    #  opt for multi-part, etc.
    f.write(data)
    f.write(another_thing)  # fail this, we can only write once.

with repo.commit('abc123').open('foo/bar.txt', 'r') as f:
    # same thing - check client for pre-signed support
    # fall back to direct read if not supported
    f.read()
    # or
    f.read(1024)  # range request

# 
# 
```

`open()` will also accept an explicit `pre_signed: Optional[bool] = None` argument.
if set, don't try and probe the client for this capability

### Import Manager

Provide a utility to run 

```python
import lakefs

main = lakefs.repository('example').branch('main')
task = main.import_from(
    's3://bucket/path', 
    destination='some/path/',
    commit_message='imported stuff!'
)
task.start()  # will not block, run the import API
task.wait()  # Block, polling in the background

# or just run(), same as start() & wait()
main.import_from(object_store_uri='s3://bucket/path', destination='datasets/', commit_message='sync datasets').run()
```

### Transaction Manager

```python
import lakefs

dev = lakefs.repository('example').branch('dev')

# Will create an ephemeral branch from `dev` (e.g. `tx-dev-343829f89`)
# uploads and downloads will apply to that ephemeral branch
# on success, commit with provided message, merge and delete ephemeral branch
# on exception or failure, leave branch as is and report it in a wrapping exception
# for easy troubleshooting
with dev.transaction(commit_message='do things') as tx:
    tx.upload('...')
    tx.download()
    # or with I/O wrapper
    tx.open('foo/bar.txt', 'w') as f:
        f.write('hello world')

```

### Creating repositories

Small helper for writing succint examples/samples:

```python
import lakefs

repo = lakefs.repository('example').get_or_create(storage_namespace='s3://bucket/path')

# From here, proceed as usual..
main = repo.branch('main')
```
