---
title: lakectl local file permissions tracking
description: An experimental feature that allows tracking file ownership and permissions when using lakectl local
parent: none
---

# lakectl local file permission tracking
{: .d-inline-block }
Experimental
{: .label .label-green }

This experimental feature is used to:
* Support mode preservation for files and folders managed via lakectl local **for POSIX compliant filesystems only**
* Support user and group preservation for files and folders managed via lakectl **local for POSIX compliant filesystems only**


{: .note}
> This feature is currently supported for Unix based Operating Systems only

{: .warning }
> Please make sure to contact the lakeFS team before enabling any experimental features!

## Configuration 

By default, this feature is disabled. Enabling file permission tracking is done via the configuration variable 
`experimental.local.posix_permissions_enabled` which can be added to the lakectl.yaml file:

   ```yaml
   credentials:
     access_key_id: AKIAIOSFDNN7EXAMPLEQ
     secret_access_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
   metastore:
     glue:
       catalog_id: ""
     hive:
       db_location_uri: file:/user/hive/warehouse/
       uri: ""
   server:
     endpoint_url: http://localhost:8000/api/v1
   experimental:
     local:
       posix_permissions:
         enabled: true
   ```

It is also possible to enable this feature via the corresponding environment variable:

```
LAKECTL_EXPERIMENTAL_LOCAL_UNIX_PERMISSIONS_ENABLED
```

## Usage

lakectl local will automatically track changes in file permissions and ownership as part of the sync with the remote server.
During the first sync with a remote path, lakectl local will need to update all the objects under this path in order to sync
the permissions with lakeFS.
When first creating the file and directory structure from an existing remote path the default permissions are set:
- `0o0666-umask` mode for files
- `0o0777-umask` mode for directories
- GID and UID of the current user
Please note that once a local path is synchronized with a remote path with permissions tracking enabled, you must continue to work with this remote path
with the feature enabled.

### Changes from default behavior

In order to support this feature, some changes needed to be made in lakectl local, and as such the behavior when the feature is enabled is a bit different from the default behavior of lakectl local.
The main differences are:
1. To support tracking of directory permissions and ownership, lakectl local will now write directory markers to the remote server, which will hold this information on the remote object.
These markers are zero sized objects with paths ending in "/" designed to indicate a directory in S3 and used to save the directory information.
2. Syncing between local and remote paths will take into account changes in file ownership and permissions. In case of change in one of these, the file will show as modified

## Limitations / Warnings

- All machines on which lakectl local is used with POSIX permissions must share UIDs and GIDs.
- Tracked modes will only be accessible via lakectl local and not through the UI, or any lakeFS API calls (e.g., statObject) or S3 gateway operations (e.g., getObject).
- Tracked modes are not expected to persist through an upload followed by an overwrite by a client other than lakectl local. After an overwrite, files and directories are expected to have their respective default mode
- The POISX permissions feature and functionality is applicable only to lakectl local scope. Using other methods to read/write/diff on paths that were cloned with the feature enabled
 will not guarantee consistent behavior compared to using the lakectl local commands. For example: performing `lakectl diff` might result in a different output than performing `lakectl local status`
- Diff (`lakectl local status`) will take into account a change permissions and ownership.The changes to permissions won’t be visualized or reported explicitly; the file will be reported as changed.
- Merge will fail on a conflict stemming from a change to permissions or ownership. There is no explicit reporting the root cause of the failure (the file will be reported as changed)
  In case of failure to read/set/update permissions (due to user insufficient privileges, for example), lakectl local will fail with an informative message. However, there's no guarantee for atomicity of operations. 
In this case, the local directory/remote path might be in an inconsistent state, and it will be up to the user to fix it.
- Unprivileged users can use non-lakectl-local commands in conjunction with lakectl local to change owners of files on remote machines in unpredictable ways,
  including in some cases to set ownership to users for which they themselves cannot chown files.

### Example

1. Clone remote repository to local path:
![clone.png](clone.png)
2. The first time we clone objects without permissions, all the files will appear as modified (note the added directory markers)
![status1.png](status1.png)
3. Let's commit the changes so that the remote path will be updated with the file permissions.
![commit.png](commit.png)
4. Looking at the remote repository we will notice the directory markers
![dir_marker.png](dir_marker.png)
5. The object metadata will contain the file/dir permissions
![object_stats.png](object_stats.png)
6. Updating a file's mode will result in a diff
![status2.png](status2.png)