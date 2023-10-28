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
    1. use `LAKECTL_SERVER_ENDPOINT_URL`, `LAKECTL_ACCESS_KEY_ID` and `LAKECTL_ACCESS_SECRET_KEY` if set
    1. Otherwise, use `~/.lakectl.yaml` if exists
    1. Otherwise, try and use IAM role from current machine (using AWS IAM role (will only work with enterprise/cloud))
1. If init is not called, it will be lazily called on the first use of `DefaultClient`, deferring authentication to the first API call.

## API wrapper interface

the higher level SDK will be resource-class based. performing API operations will be
done by calling the methods on their parent object. Examples:

```python
import lakefs

repo = lakefs.Repository('example')
branch = repo.Branch('main')

for item in branch.objects.list(prefix='foo/'):
    if item.path.endswith('.parquet'):
        print(item.path)

data: bytes = branch.Object('datasets/foo/1.parquet').open().read()
branch.Object('datasets/foo/1.parquet').upload(data)

# this will work:
data: bytes = lakefs.Repository('example').Commit('abc123').Object('a/b.txt').open().read()

# since commits are immutable, create() will not exist:
lakefs.Repository('example').Commit('abc123').Object('a/b.txt'). create(data)
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
    def create(self, storage_namespace: str, default_branch_id: str = 'main', include_samples: bool = False, exist_ok: bool = False) -> Repository: ...
    def metadata(self) -> dict[str, str]: ...
    def Branch(self, branch_id: str) -> Branch: ...
    def Commit(self, commit_id: str) -> Reference: ...
    # Ref can take a branch, tag or commit ID, returns only committed state (i.e. branch will be rev-parsed and its underlying commit returned)
    # This is asctually how the GetCommit API operation behaves, so this is essentially an alias for Commit()!
    def Ref(self, ref_id: str) -> Reference: ...  
    def Tag(self, tag_id: str) -> Tag: ...
    
    @property
    def branches(self) -> BranchManager: ...
    
    @property
    def tags(self) -> TagManager: ...


class BranchManager:
    def __init__(self, repository_id: str, client: Client = DefaultClient): ...
    def list(max_amount: Optional[int], after: str ='', prefix: str ='') -> Generator[Branch]: ...


class TagManager:
    def __init__(self, repository_id: str, client: Client = DefaultClient): ...
    def list(max_amount: Optional[int], after: str ='', prefix: str ='') -> Generator[Tag]: ...


class StoredObject:
    def __init__(self, repository_id: str, reference_id: str, path: str, client: Client = DefaultClient): ...
    def open(self, mode: Literal['r', 'rb'] = 'r', pre_signed: Optional[bool] = None) -> TextIO | BinaryIO: ...
    def stat(self) -> ObjectInfo: ...


class WritableObject(StoredObject):
    def create(self, data: bytes | str | TextIO | BinaryIO, path: str, pre_signed: Optional[bool] = None,
        content_type: Optional[str] = None, metadata: Optional[dict[str, str]] = None, 
        exist_ok: bool = False) -> ObjectInfo: ...
    def delete(self): ...
    def copy(self, to_reference: str, to_path: str): ...


class ObjectManager:
    def __init__(self, repository_id: str, reference_id: str, client: Client = DefaultClient): ...
    def list(self, max_amount: Optional[int], after: str ='', prefix: str ='', delimiter: str = '/') -> Generator[ObjectInfo | CommonPrefix]: ...


class WritableObjectManager(ObjectManager):
    def uncommitted(self, max_amount: Optional[int], after: str ='', prefix: str ='') -> Generator[Change]: ...
    def import(self, commit_message: str) -> ImportManager: ...
    def delete(self, object_paths: str | Iterable[str]): ...
    def transact(self, commit_message: str) -> Transaction: ...
    def reset_changes(self, path: Optional[str] = None): ...


class Reference:
    def __init__(self, repository_id: str, reference_id: str, client: Client = DefaultClient): ...
    def log(self, max_amount: Optional[int]) -> Generator[Reference]: ...
    def metadata(self) -> dict[str, str]: ...
    def commit_message(self) -> str: ...
    def diff(self, other_ref: str | Reference, max_amount: Optional[int], after: str ='', prefix: str ='', delimiter: str = '/') -> Generator[Change]: ...
    def merge_into(self, destination_branch_id: str | Branch): ...
    def Object(self, path: str) -> Object: ...
    @property
    def objects(self) -> ObjectManager: ...


class Branch(Reference):
    def create(self, source_reference_id: str, exist_ok: bool = False) -> Branch: ...
    def head(self) -> Reference: ...
    def commit(self, message: str, metadata: dict[str, str]) -> Reference: ...
    def delete(self): ...
    def revert(self, reference_id: str): ...
    def Object(self, path: str) -> WritableObject: ...
    @property
    def objects(self) -> WritableObjectManager: ...
    

class Tag(Reference):
    def create(self, source_reference_id: str, exist_ok: bool = False) -> Tag: ...
    def delete(self, exist_ok: bool = False): ...


class CommonPrefix:
    def __init__(self, repository_id: str, reference_id: str, path: str, client: Client = DefaultClient): ...
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


class ServerStorageConfiguration(NamedTuple):
    blockstore_type: str
    pre_sign_support: bool
    import_support: bool


class ImportManager:
    def __init__(self, repository_id: str, reference_id: str, client: Client = DefaultClient): ...
    def prefix(self, object_store_uri: str, destination: str) -> ImportManager: ...
    def object(self, object_store_uri: str, destination: str) -> ImportManager: ...
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

class Transaction(Branch):
    def __init__(self, repository_id: str, branch_id: str, client: Client = DefaultClient): ...
    def begin() -> None:
        'Create ephemeral branch from the source branch (e.g. <source_branch_id>-txn-<uuid>'
        ...

    def commit() -> Commit:
        'commit, merge, delete ephemeral branch'
        ...

    def rollback(delete_temp_branch: bool = True) -> None:
        'if delete_temp_branch = True, delete the ephemeral branch createds'

    def __enter__(self):
        'calls begin()'
        ...

    def __exit__(self, type, value, traceback):
        'if successful, commit(), otherwise rollback() and report a meaningful error'
        ...

```

While this list is fairly exhaustive, it might require a few additional tweaks and additions.

Additionally, we define the following exception hierarchy:

```python

# lakefs.exceptions
class LakeFSException(Exception):
    status_code: int
    message: str

class NotFoundException(LakeFSException): ...
class NotAuthorizedException(LakeFSException): ...
class ServerException(LakeFSException): ...
class UnsupportedOperationException(LakeFSException): ...
class ObjectNotFoundException(NotFoundException, FileNotFoundError): ...
```

Other, more specific exceptions may subclass these, but all errors returned by the lakeFS server should sub-class one of these to make error handling easier for developers.

The only exception should be errors returned by functions that, for compatibility should return specific error types. (this might be true for e.g. `open()` and errors returned by the `TextIO` or `BinaryIO` implementations).

## Higher Level Utilities

### I/O - reading/writing objects

Provide a pythonic `open()` method that returns a "file-like object" (read-only)

```python
import lakefs

repo = lakefs.Repository('example')
branch = repo.Branch('main')

# Will check the underlying client for pre-signed URL support
# if supported, will do get_physical_address->http upload->link address 
# Otherwise, will try a direct upload.
# *In the future*, we can accept a stream/file-like object, sniff for its size/content type
# opt for multi-part, etc.
branch.Object('foo/bar.txt').create(data=b'hello world!\n')

with branch.Object('foo/bar.txt').open() as f:
    data = f.read()

with repo.Commit('abc123').Object('foo/bar.txt').open() as f:
    f.read() # read all
    f.read(1024)  # or a range request

```

`open()` will also accept an explicit `pre_signed: Optional[bool] = None` argument.
if set, don't try and probe the client for this capability

### Import Manager

Provide a utility to run 

```python
import lakefs

main = lakefs.repository('example').branch('main')
task = main.objects.import(commit_message='imported stuff!'). \
    .prefix('s3://bucket/path', destination='some/path/'). \
    .prefix('s3://bucket2/other/path', destination='other/path/')

task.start()  # will not block, run the import API
task.wait()  # Block, polling in the background

# or just run(), same as start() & wait()
main.objects.import('sync datasets').prefix('s3://bucket/path/', destination='datasets/').run()
```

### Transaction Manager

```python
import lakefs

dev = lakefs.Repository('example').Branch('dev')

# Will create an ephemeral branch from `dev` (e.g. `tx-dev-343829f89`)
# uploads and downloads will apply to that ephemeral branch
# on success, commit with provided message, merge and delete ephemeral branch
# on exception or failure, leave branch as is and report it in a wrapping exception
# for easy troubleshooting
with dev.transaction('do things') as tx:
    tx.Object('foo').create(data)
    tx.Object('foo').open() as f:
        data = f.read()

```

### Creating repositories

Small helper for writing succint examples/samples:

```python
import lakefs

repo = lakefs.Repository('example').create(storage_namespace='s3://bucket/path/', exist_ok=True)

# From here, proceed as usual..
main = repo.Branch('main')
```
