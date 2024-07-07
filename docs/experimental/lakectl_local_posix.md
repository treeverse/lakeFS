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
* Support user and group preservation for files and folders managed via lakectl local **for POSIX compliant filesystems only**


{: .note}
>**Note:** This feature is currently supported for Unix based Operating Systems only

{: .warning }
>Please make sure to contact the lakeFS team before enabling any experimental features!

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
`LAKECTL_EXPERIMENTAL_LOCAL_UNIX_PERMISSIONS_ENABLED`

## Usage

lakectl local will automatically track changes in file permissions and ownership as part of the sync with the remote server.
During the first sync with a remote path, lakectl local will need to update all the objects under this path in order to sync
the permissions with lakeFS.
When first creating the file and directory structure from an existing remote path the default permissions are set:
- `0o0666 & ~umask` mode for files
- `0o0777 & ~umask` mode for directories
- Numerical GID and UID of the current user

{: .note }
>**Note:** Users with stricter umasks will change permissions on push


{: .warning }
>Please note that once a local path is synchronized with a remote path with permissions tracking enabled, you must continue to work with this remote path
with the feature enabled.

### Changes from default behavior

In order to support this feature, some changes needed to be made in lakectl local, and as such the behavior when the feature is enabled is a bit different from the default behavior of lakectl local.
The main differences are:
1. To support tracking of directory permissions and ownership, lakectl local will now write directory markers to the remote server, which will hold this information on the remote object.
These markers are zero sized objects with paths ending in `/` designed to indicate a directory in S3 and used to save the directory information.
2. Syncing between local and remote paths will take into account changes in file ownership and permissions. In case of change in one of these, the file will show as modified

## Limitations / Warnings

- All machines on which lakectl local is used with POSIX permissions must share UIDs and GIDs.
- Tracked modes will only be accessible via lakectl local and not through the UI, or any lakeFS API calls (e.g., statObject) or S3 gateway operations (e.g., getObject).
- Tracked modes are not expected to persist through an upload followed by an overwrite by a client other than lakectl local. After an overwrite, files and directories are expected to have their respective default mode
- The POSIX permissions feature and functionality is applicable only to the scope of lakectl local. Using other methods to read/write/diff on paths that were cloned with the feature enabled
 will not guarantee consistent behavior compared to using the lakectl local commands. For example: performing `lakectl diff` might result in a different output than performing `lakectl local status`
- Diff (`lakectl local status`) takes into account the following attributes:
1. Client Mtime
2. File/Dir UID
3. File/Dir GID
4. File/Dir mode  

{: .note }
>**Note:** The changes to these attributes are not visualized or reported explicitly; the file will be reported as changed.  

- Merge will fail on a conflict stemming from a change to permissions or ownership. There is no explicit reporting the root cause of the failure (the file will be reported as changed)
  In case of failure to read/set/update permissions (due to user insufficient privileges, for example), lakectl local will fail with an informative message. However, there's no guarantee for atomicity of operations. 
In this case, the local directory/remote path might be in an inconsistent state, and it will be up to the user to fix it.
- Unprivileged users can use non-lakectl-local commands in conjunction with lakectl local to change owners of files on remote machines in unpredictable ways,
  including in some cases to set ownership to users for which they themselves cannot chown files.

### Example

1. Clone remote repository to local path:
    ```shell
    user@host /tmp/test-perm
      % LAKECTL_EXPERIMENTAL_LOCAL_POSIX_PERMISSIONS_ENABLED=true lakectl local clone lakefs://quickstart/dev/     
    download data/lakes.source.md            ... done! [531B in 0s]
    download images/quickstart-step-02-bran~ ... done! [3.09KB in 0s]
    download images/quickstart-step-04-roll~ ... done! [3.22KB in 0s]
    download images/quickstart-step-03-merg~ ... done! [3.43KB in 0s]
    download images/quickstart-step-01-quer~ ... done! [3.69KB in 0s]
    download images/axolotl.png              ... done! [3.82KB in 0s]
    download images/quickstart-step-00-laun~ ... done! [4.10KB in 0s]
    download images/quickstart-step-05-acti~ ... done! [4.84KB in 0s]
    download images/waving-axolotl-transpar~ ... done! [22.92KB in 0s]
    download README.md                       ... done! [28.99KB in 0s]
    download images/create-lakefs-branch.png ... done! [56.24KB in 0s]
    download images/commit-change-02.png     ... done! [64.40KB in 0s]
    download images/duckdb-editor-05.png     ... done! [65.49KB in 0s]
    download images/merge02.png              ... done! [70.17KB in 0s]
    download images/duckdb-editor-03.png     ... done! [75.86KB in 0s]
    download images/duckdb-editor-06.png     ... done! [87.92KB in 0s]
    download images/duckdb-editor-04.png     ... done! [90.42KB in 0s]
    download images/merge01.png              ... done! [92.30KB in 0s]
    download images/commit-change.png        ... done! [98.80KB in 0s]
    download images/duckdb-editor-02.png     ... done! [103.16KB in 0s]
    download images/create-quickstart-repo.~ ... done! [121.72KB in 0s]
    download images/hooks-05.png             ... done! [139.44KB in 0s]
    download images/hooks-07.png             ... done! [142.94KB in 0s]
    download images/hooks-06.png             ... done! [146.62KB in 0s]
    download images/lakefs-login-screen.png  ... done! [148.67KB in 0s]
    download images/hooks-02.png             ... done! [151.00KB in 0s]
    download images/user_config.png          ... done! [166.49KB in 0s]
    download images/hooks-04.png             ... done! [170.31KB in 0s]
    download images/hooks-01.png             ... done! [172.13KB in 0s]
    download images/repo-list.png            ... done! [178.64KB in 0s]
    download images/hooks-00.png             ... done! [182.45KB in 0s]
    download images/hooks-03.png             ... done! [182.78KB in 0s]
    download images/duckdb-main-01.png       ... done! [206.85KB in 0s]
    download images/duckdb-main-03.png       ... done! [207.50KB in 0s]
    download images/quickstart-repo.gif      ... done! [213.80KB in 0s]
    download images/hooks-08.png             ... done! [218.91KB in 0s]
    download images/repo-contents.png        ... done! [232.40KB in 0s]
    download images/duckdb-main-02.png       ... done! [239.56KB in 0s]
    download images/empty-repo-list.png      ... done! [254.93KB in 0s]
    download lakes.parquet                   ... done! [916.39KB in 1ms]
    
    Successfully cloned lakefs://quickstart/dev/ to /tmp/test-perm.
    
    Clone Summary:
    
    Downloaded: 40
    Uploaded: 0
    Removed: 0
    
    ```

2. The first time we clone objects without permissions, all the files will appear as modified (note the added directory markers):
    ```shell
    user@host /tmp/test-perm
      % LAKECTL_EXPERIMENTAL_LOCAL_POSIX_PERMISSIONS_ENABLED=true lakectl local status                                                                                                                   !10245
    
    diff 'local:///tmp/test-perm' <--> 'lakefs://quickstart/8fa0d994afedbc00e0a44725e8a8d64695e638d4ba683756f5d2411eb7aa8fdf/'...
    diff 'lakefs://quickstart/8fa0d994afedbc00e0a44725e8a8d64695e638d4ba683756f5d2411eb7aa8fdf/' <--> 'lakefs://quickstart/dev/'...
    
    ╔════════╦══════════╦═══════════════════════════════════════════╗
    ║ SOURCE ║ CHANGE   ║ PATH                                      ║
    ╠════════╬══════════╬═══════════════════════════════════════════╣
    ║ local  ║ modified ║ README.md                                 ║
    ║ local  ║ added    ║ data/                                     ║
    ║ local  ║ modified ║ data/lakes.source.md                      ║
    ║ local  ║ added    ║ images/                                   ║
    ║ local  ║ modified ║ images/axolotl.png                        ║
    ║ local  ║ modified ║ images/commit-change-02.png               ║
    ║ local  ║ modified ║ images/commit-change.png                  ║
    ║ local  ║ modified ║ images/create-lakefs-branch.png           ║
    ║ local  ║ modified ║ images/create-quickstart-repo.png         ║
    ║ local  ║ modified ║ images/duckdb-editor-02.png               ║
    ║ local  ║ modified ║ images/duckdb-editor-03.png               ║
    ║ local  ║ modified ║ images/duckdb-editor-04.png               ║
    ║ local  ║ modified ║ images/duckdb-editor-05.png               ║
    ║ local  ║ modified ║ images/duckdb-editor-06.png               ║
    ║ local  ║ modified ║ images/duckdb-main-01.png                 ║
    ║ local  ║ modified ║ images/duckdb-main-02.png                 ║
    ║ local  ║ modified ║ images/duckdb-main-03.png                 ║
    ║ local  ║ modified ║ images/empty-repo-list.png                ║
    ║ local  ║ modified ║ images/hooks-00.png                       ║
    ║ local  ║ modified ║ images/hooks-01.png                       ║
    ║ local  ║ modified ║ images/hooks-02.png                       ║
    ║ local  ║ modified ║ images/hooks-03.png                       ║
    ║ local  ║ modified ║ images/hooks-04.png                       ║
    ║ local  ║ modified ║ images/hooks-05.png                       ║
    ║ local  ║ modified ║ images/hooks-06.png                       ║
    ║ local  ║ modified ║ images/hooks-07.png                       ║
    ║ local  ║ modified ║ images/hooks-08.png                       ║
    ║ local  ║ modified ║ images/lakefs-login-screen.png            ║
    ║ local  ║ modified ║ images/merge01.png                        ║
    ║ local  ║ modified ║ images/merge02.png                        ║
    ║ local  ║ modified ║ images/quickstart-repo.gif                ║
    ║ local  ║ modified ║ images/quickstart-step-00-launch.png      ║
    ║ local  ║ modified ║ images/quickstart-step-01-query.png       ║
    ║ local  ║ modified ║ images/quickstart-step-02-branch.png      ║
    ║ local  ║ modified ║ images/quickstart-step-03-merge.png       ║
    ║ local  ║ modified ║ images/quickstart-step-04-rollback.png    ║
    ║ local  ║ modified ║ images/quickstart-step-05-actions.png     ║
    ║ local  ║ modified ║ images/repo-contents.png                  ║
    ║ local  ║ modified ║ images/repo-list.png                      ║
    ║ local  ║ modified ║ images/user_config.png                    ║
    ║ local  ║ modified ║ images/waving-axolotl-transparent-w90.gif ║
    ║ local  ║ modified ║ lakes.parquet                             ║
    ╚════════╩══════════╩═══════════════════════════════════════════╝
    
    ```

3. Let's commit the changes so that the remote path will be updated with the file permissions:

    ```shell
    user@host /tmp/test-perm
      % LAKECTL_EXPERIMENTAL_LOCAL_POSIX_PERMISSIONS_ENABLED=true lakectl local commit -m "Add file permissions"                                                                                         !10246
    
    Getting branch: dev
    
    diff 'local:///tmp/test-perm' <--> 'lakefs://quickstart/8fa0d994afedbc00e0a44725e8a8d64695e638d4ba683756f5d2411eb7aa8fdf/'...
    upload data/                             ... done! [0B in 15ms]
    upload images/                           ... done! [0B in 15ms]
    upload data/lakes.source.md              ... done! [531B in 11ms]
    upload images/quickstart-step-02-branch~ ... done! [3.09KB in 11ms]
    upload images/quickstart-step-04-rollba~ ... done! [3.22KB in 11ms]
    upload images/quickstart-step-03-merge.~ ... done! [3.43KB in 12ms]
    upload images/quickstart-step-01-query.~ ... done! [3.69KB in 11ms]
    upload images/axolotl.png                ... done! [3.82KB in 15ms]
    upload images/quickstart-step-00-launch~ ... done! [4.10KB in 12ms]
    upload images/quickstart-step-05-action~ ... done! [4.84KB in 11ms]
    upload images/waving-axolotl-transparen~ ... done! [22.92KB in 11ms]
    upload README.md                         ... done! [28.99KB in 15ms]
    upload images/create-lakefs-branch.png   ... done! [56.24KB in 15ms]
    upload images/commit-change-02.png       ... done! [64.40KB in 14ms]
    upload images/duckdb-editor-05.png       ... done! [65.49KB in 14ms]
    upload images/merge02.png                ... done! [70.17KB in 12ms]
    upload images/duckdb-editor-03.png       ... done! [75.86KB in 16ms]
    upload images/duckdb-editor-06.png       ... done! [87.92KB in 13ms]
    upload images/duckdb-editor-04.png       ... done! [90.42KB in 15ms]
    upload images/merge01.png                ... done! [92.30KB in 12ms]
    upload images/commit-change.png          ... done! [98.80KB in 15ms]
    upload images/duckdb-editor-02.png       ... done! [103.16KB in 12ms]
    upload images/create-quickstart-repo.png ... done! [121.72KB in 15ms]
    upload images/hooks-05.png               ... done! [139.44KB in 15ms]
    upload images/hooks-07.png               ... done! [142.94KB in 11ms]
    upload images/hooks-06.png               ... done! [146.62KB in 13ms]
    upload images/lakefs-login-screen.png    ... done! [148.67KB in 12ms]
    upload images/hooks-02.png               ... done! [151.00KB in 13ms]
    upload images/user_config.png            ... done! [166.49KB in 11ms]
    upload images/hooks-04.png               ... done! [170.31KB in 14ms]
    upload images/hooks-01.png               ... done! [172.13KB in 13ms]
    upload images/repo-list.png              ... done! [178.64KB in 11ms]
    upload images/hooks-00.png               ... done! [182.45KB in 13ms]
    upload images/hooks-03.png               ... done! [182.78KB in 13ms]
    upload images/duckdb-main-01.png         ... done! [206.85KB in 13ms]
    upload images/duckdb-main-03.png         ... done! [207.50KB in 15ms]
    upload images/quickstart-repo.gif        ... done! [213.80KB in 12ms]
    upload images/hooks-08.png               ... done! [218.91KB in 12ms]
    upload images/repo-contents.png          ... done! [232.40KB in 11ms]
    upload images/duckdb-main-02.png         ... done! [239.56KB in 14ms]
    upload images/empty-repo-list.png        ... done! [254.93KB in 13ms]
    upload lakes.parquet                     ... done! [916.39KB in 14ms]
    
    Sync Summary:
    
    Downloaded: 0
    Uploaded: 42
    Removed: 0
    
    Finished syncing changes. Perform commit on branch...
    Commit for branch "dev" completed.
    
    ID: 7f1f0984030c43ab388a649fe97c3c1564421118099ccc8c47f74645776e73f2
    Message: Add file permissions
    Timestamp: 2024-07-03 16:04:07 +0300 IDT
    Parents: 8fa0d994afedbc00e0a44725e8a8d64695e638d4ba683756f5d2411eb7aa8fdf
    
    ```

4. Looking at the remote repository we will notice the directory markers:
![dir_marker.png](dir_marker.png)

5. The object metadata will contain the file/dir permissions:
![object_stats.png](object_stats.png)

6. Updating a file's mode will result in a diff:
    
    ```shell
    user@host /tmp/test-perm
      % chmod 770 lakes.parquet                                                                                                                                                                          !10247
    
    user@host /tmp/test-perm
      % LAKECTL_EXPERIMENTAL_LOCAL_POSIX_PERMISSIONS_ENABLED=true lakectl local status                                                                                                                   !10248
    
    diff 'local:///tmp/test-perm' <--> 'lakefs://quickstart/7f1f0984030c43ab388a649fe97c3c1564421118099ccc8c47f74645776e73f2/'...
    diff 'lakefs://quickstart/7f1f0984030c43ab388a649fe97c3c1564421118099ccc8c47f74645776e73f2/' <--> 'lakefs://quickstart/dev/'...
    
    ╔════════╦══════════╦═══════════════╗
    ║ SOURCE ║ CHANGE   ║ PATH          ║
    ╠════════╬══════════╬═══════════════╣
    ║ local  ║ modified ║ lakes.parquet ║
    ╚════════╩══════════╩═══════════════╝
    
    ```