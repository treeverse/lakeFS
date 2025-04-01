# lakeFS Repository References Management

This script provides functionality to dump and restore lakeFS repository references, which is particularly useful for repository migration and backup purposes.

## Prerequisites

Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Dump Repository References

Dump a single repository:

```bash
python lakefs-refs.py dump my-repository
```

Dump a single repository with committing changes:

```bash
python lakefs-refs.py dump my-repository --commit
```

Dump a single repository and delete repository record after successful dump:

```bash
python lakefs-refs.py dump my-repository --rm
```

Dump all repositories:

```bash
python lakefs-refs.py dump --all
```

Dump all repositories with committing changes:

```bash
python lakefs-refs.py dump --all --commit
```

Dump all repositories and delete them after successful dump:

```bash
python lakefs-refs.py dump --all --rm
```

### Restore Repository References

Restore a single repository:

```bash
python lakefs-refs.py restore my-repository_manifest.json
```

Restore multiple repositories:

```bash
python lakefs-refs.py restore repo1_manifest.json repo2_manifest.json repo3_manifest.json
```

Restore all manifest files in the current directory:

```bash
python lakefs-refs.py restore *_manifest.json
```

Restore without storage_id (useful when migrating to a different storage backend):

```bash
python lakefs-refs.py restore my-repository_manifest.json --ignore-storage-id
```

## Authentication

The script supports authentication through:

1. Command line arguments:

   ```bash
   python lakefs-refs.py dump my-repository --endpoint-url http://localhost:8000 --access-key-id my-key --secret-access-key my-secret
   ```

2. Environment variables:

   ```bash
   export LAKECTL_SERVER_ENDPOINT_URL=http://localhost:8000
   export LAKECTL_CREDENTIALS_ACCESS_KEY_ID=my-key
   export LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY=my-secret
   ```

3. Use `$HOME/.lakectl.yaml` configuration file:

   ```yaml
   server:
     endpoint_url: http://localhost:8000
   credentials:
     access_key_id: my-key
     secret_access_key: my-secret
   ```
