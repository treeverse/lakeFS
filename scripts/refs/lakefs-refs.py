#!/usr/bin/env python3
"""
lakeFS Repository References Management Script

This script provides functionality to dump and restore lakeFS repository references:
1. Dump: Dumps repository metadata (refs) and optionally commits uncommitted changes
2. Restore: Creates a bare repository if needed and restores repository metadata from a manifest file

Usage:
    python refs.py dump [<repository_name>] [--all] [--commit] [--endpoint-url <lakefs_endpoint>] [--access-key-id <access_key>] [--secret-access-key <secret_key>]
    python refs.py restore --manifest <manifest_file> [--endpoint-url <lakefs_endpoint>] [--access-key-id <access_key>] [--secret-access-key <secret_key>] [--repository <new_repo_name>]
"""

import datetime
import sys
import json
import argparse
import os
import yaml
import lakefs_sdk
from lakefs_sdk.client import LakeFSClient


def load_config_from_lakectl():
    """
    Load configuration from ~/.lakectl.yaml if it exists.
    Falls back to environment variables if config file not found or values missing.
    
    Returns:
        dict: Configuration with endpoint_url, access_key_id, and secret_access_key if found, empty dict otherwise
    """
    result = {}
    
    # Try loading from config file first (lowest priority)
    config_path = os.path.expanduser("~/.lakectl.yaml")
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            if config and 'credentials' in config:
                if 'access_key_id' in config['credentials']:
                    result['access_key_id'] = config['credentials']['access_key_id']
                if 'secret_access_key' in config['credentials']:
                    result['secret_access_key'] = config['credentials']['secret_access_key']
            
            if config and 'server' in config and 'endpoint_url' in config['server']:
                result['endpoint_url'] = config['server']['endpoint_url']
        except Exception as e:
            print(f"Warning: Failed to load config from {config_path}: {str(e)}")
    
    # Check environment variables and use as fallback (medium priority)
    env_access_key = os.environ.get('LAKECTL_CREDENTIALS_ACCESS_KEY_ID')
    if env_access_key:
        result['access_key_id'] = env_access_key
            
    env_secret_key = os.environ.get('LAKECTL_CREDENTIALS_SECRET_ACCESS_KEY')
    if env_secret_key:
        result['secret_access_key'] = env_secret_key
            
    env_endpoint = os.environ.get('LAKECTL_SERVER_ENDPOINT_URL')
    if env_endpoint:
        result['endpoint_url'] = env_endpoint
    
    return result

def create_lakefs_client(endpoint_url=None, access_key_id=None, secret_access_key=None):
    """
    Create a lakeFS client with the provided credentials.
    
    Args:
        endpoint_url: lakeFS API endpoint URL
        access_key_id: lakeFS access key ID
        secret_access_key: lakeFS secret access key
    
    Returns:
        LakeFSClient: Configured lakeFS client
    """
    # Load config from lakectl.yaml if available
    config = load_config_from_lakectl()
    
    # Create configuration with provided credentials if available
    if config:
        configuration = lakefs_sdk.Configuration()
        
        # Command line arguments have highest priority
        if endpoint_url:
            configuration.host = endpoint_url
        elif 'endpoint_url' in config:
            configuration.host = config['endpoint_url']
        
        if access_key_id and secret_access_key:
            configuration.username = access_key_id
            configuration.password = secret_access_key
        elif 'access_key_id' in config and 'secret_access_key' in config:
            configuration.username = config['access_key_id']
            configuration.password = config['secret_access_key']
        
        return LakeFSClient(configuration=configuration)
    
    # If no config available, use command line arguments if provided
    if endpoint_url or (access_key_id and secret_access_key):
        configuration = lakefs_sdk.Configuration()
        if endpoint_url:
            configuration.host = endpoint_url
        if access_key_id and secret_access_key:
            configuration.username = access_key_id
            configuration.password = secret_access_key
        return LakeFSClient(configuration=configuration)
    
    return LakeFSClient()


def paginate_results(api_call, **kwargs):
    """
    Generic pagination function that yields results from any paginated API call.
    
    Args:
        api_call: Function to call that returns paginated results
        **kwargs: Additional arguments to pass to the API call
    
    Yields:
        Any: Each result as it's retrieved
    """
    after = None
    while True:
        result = api_call(after=after, **kwargs)
        for item in result.results:
            yield item
        if not result.pagination.has_more:
            break
        after = result.pagination.next_offset


def dump_repository(repo_name, endpoint_url=None, access_key_id=None, secret_access_key=None, commit=False, delete_after_dump=False):
    """
    Backup a lakeFS repository by committing changes and dumping metadata.
    
    Args:
        repo_name: Name of the repository to backup
        endpoint_url: lakeFS API endpoint URL
        access_key_id: lakeFS access key ID
        secret_access_key: lakeFS secret access key
        commit: If True, commit uncommitted changes before dumping
        delete_after_dump: If True, delete the repository after successful dump
    
    Returns:
        bool: True if backup was successful, False otherwise
    """
    try:
        # Initialize client with provided credentials
        lakefs_client = create_lakefs_client(endpoint_url, access_key_id, secret_access_key)
        
        # Get repository info to extract storage namespace
        repo_info = lakefs_client.repositories_api.get_repository(repo_name)
        storage_namespace = repo_info.storage_namespace
        default_branch = repo_info.default_branch
        storage_id = repo_info.storage_id
        
        # Commit any uncommitted changes in all branches if requested
        if commit:
            # Get all branches
            print(f"Checking for uncommitted changes in repository: {repo_name}")
            
            for branch in paginate_results(lakefs_client.branches_api.list_branches, repository=repo_name):
                branch_id = branch.id
                # Get uncommitted changes
                uncommitted = lakefs_client.branches_api.diff_branch(
                    repository=repo_name,
                    branch=branch_id,
                )
                
                # If there are uncommitted changes, commit them
                if uncommitted.results:
                    print(f"Found {len(uncommitted.results)} uncommitted changes in branch {branch_id}")
                    lakefs_client.commits_api.commit(
                        repository=repo_name,
                        branch=branch_id,
                        commit_creation={
                            "message": "Committed changes for dump",
                            "metadata": {"dump": datetime.datetime.now().isoformat()}
                        }
                    )
                    print(f"Committed changes in branch {branch_id}")
                else:
                    print(f"No uncommitted changes in branch {branch_id}")
        
        # Dump repository metadata
        print(f"Dumping repository metadata...")
        refs_manifest = lakefs_client.internal_api.dump_refs(repo_name)
        
        # Create repository manifest with repository info and refs
        repository_manifest = {
            "repository": {
                "name": repo_name,
                "storage_namespace": storage_namespace,
                "default_branch": default_branch,
                "storage_id": storage_id
            },
            "refs": refs_manifest.to_dict()
        }
        
        # Print repository information and refs_manifest location
        print(f"Repository: {repo_name}")
        print(f"Storage Namespace: {storage_namespace}")
        if storage_id:
            print(f"Storage ID: {storage_id}")
        print(f"Default Branch: {default_branch}")
        
        # Save the repository manifest to a local file
        output_file = f"{repo_name}_manifest.json"
        with open(output_file, 'w') as f:
            json.dump(repository_manifest, f, indent=2)
        print(f"Repository manifest saved to local file: {output_file}")
        
        # Delete repository if requested
        if delete_after_dump:
            print(f"Deleting repository: {repo_name}")
            lakefs_client.repositories_api.delete_repository(repo_name)
            print(f"Repository deleted successfully")
        
        return True
    
    except Exception as e:
        print(f"Error dumping repository {repo_name}: {str(e)}")
        return False


def dump_all_repositories(endpoint_url=None, access_key_id=None, secret_access_key=None, commit=False, delete_after_dump=False):
    """
    Backup all lakeFS repositories by committing changes and dumping metadata.
    
    Args:
        endpoint_url: lakeFS API endpoint URL
        access_key_id: lakeFS access key ID
        secret_access_key: lakeFS secret access key
        commit: If True, commit uncommitted changes before dumping
        delete_after_dump: If True, delete each repository after successful dump
    
    Returns:
        bool: True if all backups were successful, False otherwise
    """
    try:
        # Initialize client with provided credentials
        lakefs_client = create_lakefs_client(endpoint_url, access_key_id, secret_access_key)
        
        # Get all repositories
        print("Fetching all repositories...")
        repos_found = False
        success = True
        
        for repo in paginate_results(lakefs_client.repositories_api.list_repositories):
            repos_found = True
            print(f"\nProcessing repository: {repo.id}")
            if not dump_repository(repo.id, endpoint_url, access_key_id, secret_access_key, commit, delete_after_dump):
                success = False
                print(f"Failed to dump repository: {repo.id}")
        
        if not repos_found:
            print("No repositories found.")
        
        return success
    
    except Exception as e:
        print(f"Error dumping repositories: {str(e)}")
        return False


def restore_repository(manifest_file, endpoint_url=None, access_key_id=None, secret_access_key=None, ignore_storage_id=False):
    """
    Restore a lakeFS repository from a manifest file.
    
    Args:
        manifest_file: Path to the manifest file
        endpoint_url: lakeFS API endpoint URL
        access_key_id: lakeFS access key ID
        secret_access_key: lakeFS secret access key
        ignore_storage_id: If True, create repository without storage_id
    
    Returns:
        bool: True if restore was successful, False otherwise
    """
    try:
        # Initialize client with provided credentials
        lakefs_client = create_lakefs_client(endpoint_url, access_key_id, secret_access_key)
        
        # Read the manifest file
        with open(manifest_file, 'r') as f:
            manifest = json.load(f)
        
        # Extract repository info and refs
        repo_info = manifest["repository"]
        refs_data = manifest["refs"]
        
        # Create repository
        repo_name = repo_info["name"]
        storage_namespace = repo_info["storage_namespace"]
        default_branch = repo_info["default_branch"]
        storage_id = repo_info.get("storage_id", "")  # Default to empty string if not found
        
        # Create repository with or without storage_id
        repo_creation = {
            "name": repo_name,
            "storage_namespace": storage_namespace,
            "default_branch": default_branch
        }
        
        if not ignore_storage_id:
            repo_creation["storage_id"] = storage_id
        
        lakefs_client.repositories_api.create_repository(repo_creation, bare=True)
        print(f"Created repository: {repo_name}")
        
        # Restore refs
        lakefs_client.internal_api.restore_refs(repo_name, refs_data)
        print(f"Restored refs for repository: {repo_name}")
        
        return True
    
    except Exception as e:
        print(f"Error restoring repository from {manifest_file}: {str(e)}")
        return False


def main():
    """Main function to parse arguments and run the appropriate operation"""
    parser = argparse.ArgumentParser(description='lakeFS Repository References Management')
    
    # Common arguments for all commands
    parser.add_argument('--endpoint-url', help='lakeFS API endpoint URL')
    parser.add_argument('--access-key-id', help='lakeFS access key ID')
    parser.add_argument('--secret-access-key', help='lakeFS secret access key')
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Dump command
    dump_parser = subparsers.add_parser('dump', help='Dump repository references')
    dump_group = dump_parser.add_mutually_exclusive_group(required=True)
    dump_group.add_argument('repository', nargs='?', help='Name of the repository to dump')
    dump_group.add_argument('--all', action='store_true', help='Dump all repositories')
    dump_parser.add_argument('--commit', action='store_true', help='Commit uncommitted changes before dumping')
    dump_parser.add_argument('--rm', action='store_true', help='Delete repository after successful dump')
    
    # Restore command
    restore_parser = subparsers.add_parser('restore', help='Restore repository references')
    restore_parser.add_argument('manifests', nargs='+', help='One or more manifest files to restore')
    restore_parser.add_argument('--ignore-storage-id', action='store_true', help='Create repository without storage_id')
    
    args = parser.parse_args()
    
    if args.command == 'dump':
        if args.all:
            success = dump_all_repositories(
                endpoint_url=args.endpoint_url,
                access_key_id=args.access_key_id,
                secret_access_key=args.secret_access_key,
                commit=args.commit,
                delete_after_dump=args.rm
            )
        else:
            success = dump_repository(
                args.repository,
                endpoint_url=args.endpoint_url,
                access_key_id=args.access_key_id,
                secret_access_key=args.secret_access_key,
                commit=args.commit,
                delete_after_dump=args.rm
            )
    elif args.command == 'restore':
        success = True
        for manifest in args.manifests:
            if not restore_repository(
                manifest,
                endpoint_url=args.endpoint_url,
                access_key_id=args.access_key_id,
                secret_access_key=args.secret_access_key,
                ignore_storage_id=args.ignore_storage_id
            ):
                success = False
                print(f"Failed to restore repository from manifest: {manifest}")
    else:
        parser.print_help()
        return 1
    
    if success:
        print(f"\nOperation '{args.command}' completed successfully.")
        return 0
    else:
        print(f"\nOperation '{args.command}' failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main()) 
