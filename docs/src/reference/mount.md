---
title: Mount (Everest)
description: Mount a lakeFS path to your local filesystem or in Kubernetes.
status: enterprise
---

# Mount (Everest)

!!! info
    Available in **lakeFS Cloud** and **lakeFS Enterprise**

Everest is a complementary binary to lakeFS that allows you to virtually mount a remote lakeFS repository onto a local directory or within a Kubernetes environment. Once mounted, you can access data as if it resides on your local filesystem, using any tool, library, or framework.

!!! tip
    Everest mount supports writing to the file system for both NFS and FUSE protocols starting version **0.2.0**! Read more about [Write Mode Operations](#write-mode-operations).

<iframe width="560" height="315" src="https://www.youtube.com/embed/BgKuoa8LAaU" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture" allowfullscreen></iframe>

## Use Cases

* **Simplified Data Loading**: Use your existing tools to read and write files directly from the filesystem with no need for custom data loaders or SDKs.
* **Seamless Scalability**: Scale from a few local files to billions without changing your tools or workflow. Use the same code from experimentation to production.
* **Enhanced Performance**: Everest supports billions of files and offers fast, lazy data fetching, making it ideal for optimizing GPU utilization and other performance-sensitive tasks.

---

## Getting Started (Local Mount)

This guide will walk you through setting up and using Everest to mount a lakeFS repository on your local machine.

### 1. Prerequisites

*   lakeFS Cloud account or lakeFS Enterprise Version `1.25.0` or higher.
*   **Supported OS:** macOS (with NFS V3) or Linux.
*   **Get the Everest Binary:** Everest is a self-contained binary with no installation required. Please [contact us](http://info.lakefs.io/thanks-lakefs-mounts) to get access.

### 2. Authentication & Configuration

Everest uses the same configuration and authentication methods as `lakectl`. It discovers credentials and the server endpoint in the following order:

1.  **Command-Line Flags:** `--lakectl-access-key-id`, `--lakectl-secret-access-key`, and `--lakectl-server-url`.
2.  **Environment Variables:** `LAKECTL_*` or `EVEREST_LAKEFS_*` prefixed variables.
3.  **Configuration File:** `~/.lakectl.yaml` (or the file specified by `--lakectl-config`).

#### Authentication Methods

Everest will attempt to authenticate in the following order:

1.  **Session Token:** From `EVEREST_LAKEFS_CREDENTIALS_SESSION_TOKEN` or `LAKECTL_CREDENTIALS_SESSION_TOKEN`.
2.  **lakeFS Key Pair:** Standard access key ID and secret access key.
3.  **IAM Authentication:** If your lakeFS environment is configured for [AWS IAM Role Login](../security/external-principals-aws.md), Everest (≥ v0.4.0) can authenticate using your AWS environment (e.g., `AWS_PROFILE`). To enable this, [configure your .lakectl.yaml](../security/external-principals-aws.md#lakectl-configuration) with `provider_type: aws_iam`.

### 3. Your First Mount (Read-Only)

Let's mount a prefix from a lakeFS repository to a local directory. In read-only mode, Everest mounts a specific commit ID. If you provide a branch name, it will resolve to the HEAD commit at the time of mounting.

1.  **Mount the repository:**
    This command mounts the `datasets/pets/` prefix from the `main` branch of the `image-repo` repository into a new local directory named `./pets`.

    ```bash
    everest mount "lakefs://image-repo/main/datasets/pets/" "./pets"
    ```

2.  **Explore the data:**
    You can now use standard filesystem commands to interact with your data. Files are downloaded lazily only when you access their content.

    ```bash
    # List files - this only fetches metadata
    ls -l "./pets/dogs/"

    # Find files
    find ./pets -name "*.small.jpg"

    # Open a file - this triggers a download
    open -a Preview "./pets/dogs/golden_retrievers/cute.jpg"
    ```

3.  **Unmount the directory:**
    When you are finished, unmount the directory.

    ```bash
    everest umount "./pets"
    ```

---

## Working with Data (Local Mount)

### Read-Only Operations

Read-only mode is the default and is ideal for data exploration, analysis, and feeding data into local applications without the risk of accidental changes.

!!! example "Working with Data Locally"
    Mount a repository and use your favorite tools directly on the data.

    ```bash
    everest mount lakefs://image-repo/main/datasets/pets/ ./pets

    # Run a python script
    pytorch_train.py --input ./pets

    # Query data with DuckDB
    duckdb "SELECT * FROM read_parquet('pets/labels.parquet')"

    everest umount ./pets
    ```

### Write-Mode Operations

By enabling write mode, you can modify, add, and delete files locally and then commit those changes back to the lakeFS branch.

!!! warning
    When running in write mode, the lakeFS URI must point to a branch, not a commit ID or a tag.

!!! example "Changing Data Locally"
    1.  **Mount in write mode:**
        Use the `--write-mode` flag to enable writes.

        ```bash
        everest mount lakefs://image-repo/main/datasets/pets/ ./pets --write-mode
        ```

    2.  **Modify files:**
        Make any changes you need using standard shell commands.

        ```bash
        # Add a new file
        echo "new data" > ./pets/birds/parrot/cute.jpg

        # Update an existing file
        echo "new data" >> ./pets/dogs/golden_retrievers/cute.jpg

        # Delete a file
        rm ./pets/cats/persian/cute.jpg
        ```

    3.  **Review your changes:**
        The `diff` command shows the difference between your local state and the branch's state at the time of mounting.

        ```bash
        everest diff ./pets
        # Output:
        # + added datasets/pets/birds/parrot/cute.jpg
        # ~ modified datasets/pets/dogs/golden_retrievers/cute.jpg
        # - removed datasets/pets/cats/persian/cute.jpg
        ```

    4.  **Commit your changes:**
        The `commit` command uploads your local changes and commits them to the source branch in lakeFS.

        ```bash
        everest commit ./pets -m "Updated pet images"
        ```
        After committing, your local mount will be synced to the new HEAD of the branch. Running `diff` again will show no changes.

    5.  **Unmount when finished:**
        ```bash
        everest umount ./pets
        ```

---

## Everest on Kubernetes (CSI Driver)

!!! warning "Private Preview"
    The CSI Driver is in private preview. Please [contact us](http://info.lakefs.io/thanks-lakefs-mounts) to get access.

The lakeFS CSI (Container Storage Interface) Driver allows Kubernetes Pods to mount and interact with data in a lakeFS repository as if it were a local filesystem.

### How it Works

The CSI driver, installed in your cluster, orchestrates mount operations on each Kubernetes node. It does not execute `mount` commands directly. Instead, it communicates via a Unix socket with a `systemd` service running on the host. This service is responsible for executing the `everest mount` and `umount` commands, making lakeFS URIs available to Pods as persistent volumes.

### Status and Limitations

*   **Tested OS:** BottleRocket-OS, Amazon Linux 2, RHEL 8.
*   **Kubernetes:** Version `>=1.23.0`.
*   **Provisioning:** Static provisioning only.
*   **Access Modes:** `ReadOnlyMany` is supported.
*   **Security Context:** Setting Pod `securityContext` (e.g., `runAsUser`) is not currently supported.

### 1. Prerequisites

1.  lakeFS Enterprise Version `1.25.0` or higher.
2.  A Kubernetes cluster (`>=1.23.0`) with [Helm](https://helm.sh/docs/intro/install/) installed.
3.  Network access from the cluster pods to your lakeFS server.
4.  Access to the `treeverse/everest-lakefs-csi-driver` Docker Hub image.

### 2. Deploy the CSI Driver

The driver is deployed using a Helm chart.

1.  **Add the lakeFS Helm repository:**
    ```bash
    helm repo add lakefs https://charts.lakefs.io
    helm repo update lakefs
    ```
    Verify the chart is available and see the latest version:
    ```bash
    helm search repo lakefs/everest-lakefs-csi-driver
    ```
    To see all available chart versions, use the `-l` flag:
    ```bash
    helm search repo lakefs/everest-lakefs-csi-driver -l
    ```

2.  **Configure `values.yaml`:**
    Create a `values.yaml` file to configure the driver. At a minimum, you must provide credentials for Docker Hub and your lakeFS server.
    You can view the complete list of configuration options by running `helm show values lakefs/everest-lakefs-csi-driver --version <version>`.

    !!! example "`values.yaml` example"
        ```yaml
        # Docker Hub credentials to pull the CSI driver image
        imagePullSecret:
          token: <dockerhub-token>
          username: <dockerhub-user>

        # Default lakeFS credentials for Everest to use when mounting volumes
        lakeFSAccessSecret:
          keyId: <lakefs-key-id>
          accessKey: <lakefs-access-key>
          endpoint: <lakefs-endpoint>

        node:
          # Logging verbosity (0-4 is normal, 5 is most verbose)
          logLevel: 4
          # (Advanced) Only set if you have issues with the Everest binary installation on the node.
          # This path must end with a "/"
          # everestInstallPath: /opt/everest-mount/bin/

        # (Advanced) Additional environment variables for the CSI driver pod
        # extraEnvVars:
        #   - name: CSI_DRIVER_MOUNT_TIMEOUT
        #     value: "30s"
        ```

3.  **Install the chart:**
    ```bash
    helm install -f values.yaml lakefs lakefs/everest-lakefs-csi-driver --version <chart-version>
    ```

### 3. Use in Pods

To use the driver, you create a `PersistentVolume` (PV) and a `PersistentVolumeClaim` (PVC) to mount a lakeFS URI into your Pod.

*   **Static Provisioning:** You must set `storageClassName: ""` in your PVC. To ensure a PVC is bound to a specific PV, you can use a `claimRef` in the PV definition to create a one-to-one mapping.
*   **Mount URI:** The `lakeFSMountUri` is the only required attribute in the PV spec.
*   **Mount Options:** Additional `everest mount` flags can be passed via `mountOptions` in the PV spec.

#### Examples

The following examples demonstrate how to mount a lakeFS URI in different Kubernetes scenarios.

=== "Single Pod and Mount"

    This example mounts a single lakeFS URI into one Pod.
    ```yaml
    apiVersion: v1
    kind: PersistentVolume
    metadata:
      name: everest-pv
    spec:
      capacity:
        storage: 100Gi # Required by Kubernetes, but ignored by Everest
      accessModes:
        - ReadOnlyMany
      csi:
        driver: csi.everest.lakefs.io
        volumeHandle: everest-csi-driver-volume-1 # Must be unique
        volumeAttributes:
          # Replace with your lakeFS mount URI
          lakeFSMountUri: lakefs://<repo>/<ref>/<path>
    ---
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: everest-claim
    spec:
      accessModes:
        - ReadOnlyMany
      storageClassName: "" # Required for static provisioning
      resources:
        requests:
          storage: 5Gi # Required by Kubernetes, but ignored by Everest
      volumeName: everest-pv
    ---
    apiVersion: v1
    kind: Pod
    metadata:
      name: everest-app
    spec:
      containers:
        - name: app
          image: centos
          command: ["/bin/sh", "-c", "ls /data/; tail -f /dev/null"]
          volumeMounts:
            - name: my-lakefs-data
              mountPath: /data
      volumes:
        - name: my-lakefs-data
          persistentVolumeClaim:
            claimName: everest-claim
    ```

=== "Multiple Pods, One Mount (Deployment)"

    A Deployment where multiple Pods share the same lakeFS mount. Each Pod gets its own independent mount.
    ```yaml
    apiVersion: v1
    kind: PersistentVolume
    metadata:
      name: multiple-pods-one-pv
    spec:
      capacity:
        storage: 100Gi
      accessModes:
        - ReadOnlyMany
      csi:
        driver: csi.everest.lakefs.io
        volumeHandle: everest-csi-driver-volume-2 # Must be unique
        volumeAttributes:
          lakeFSMountUri: lakefs://<repo>/<ref>/<path>
    ---
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: multiple-pods-one-claim
    spec:
      accessModes:
        - ReadOnlyMany
      storageClassName: ""
      resources:
        requests:
          storage: 5Gi
      volumeName: multiple-pods-one-pv
    ---
    apiVersion: apps/v1
    kind: Deployment
    metadata:
      name: multi-pod-app
    spec:
      replicas: 3
      selector:
        matchLabels:
          app: multi-pod-app
      template:
        metadata:
          labels:
            app: multi-pod-app
        spec:
          containers:
          - name: app
            image: centos
            command: ["/bin/sh", "-c", "ls /data/; tail -f /dev/null"]
            volumeMounts:
            - name: lakefs-storage
              mountPath: /data
          volumes:
          - name: lakefs-storage
            persistentVolumeClaim:
              claimName: multiple-pods-one-claim
    ```

=== "Multiple Mounts, Single Pod"

    A single Pod with two different lakeFS URIs mounted to two different paths.
    ```yaml
    # Define two PVs and two PVCs, one for each mount.
    # PV 1
    apiVersion: v1
    kind: PersistentVolume
    metadata:
      name: multi-mount-pv-1
    spec:
      capacity: { storage: 100Gi }
      accessModes: [ReadOnlyMany]
      csi:
        driver: csi.everest.lakefs.io
        volumeHandle: everest-csi-driver-volume-3 # Must be unique
        volumeAttributes:
          lakeFSMountUri: lakefs://<repo>/<ref>/<path1>
    ---
    # PVC 1
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: multi-mount-claim-1
    spec:
      accessModes: [ReadOnlyMany]
      storageClassName: ""
      resources: { requests: { storage: 5Gi } }
      volumeName: multi-mount-pv-1
    ---
    # PV 2
    apiVersion: v1
    kind: PersistentVolume
    metadata:
      name: multi-mount-pv-2
    spec:
      capacity: { storage: 100Gi }
      accessModes: [ReadOnlyMany]
      csi:
        driver: csi.everest.lakefs.io
        volumeHandle: everest-csi-driver-volume-4 # Must be unique
        volumeAttributes:
          lakeFSMountUri: lakefs://<repo>/<ref>/<path2>
    ---
    # PVC 2
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      name: multi-mount-claim-2
    spec:
      accessModes: [ReadOnlyMany]
      storageClassName: ""
      resources: { requests: { storage: 5Gi } }
      volumeName: multi-mount-pv-2
    ---
    # Pod
    apiVersion: v1
    kind: Pod
    metadata:
      name: multi-mount-pod
    spec:
      containers:
        - name: app
          image: centos
          command: ["/bin/sh", "-c", "echo 'Path 1:'; ls /data1; echo 'Path 2:'; ls /data2; tail -f /dev/null"]
          volumeMounts:
            - name: lakefs-data-1
              mountPath: /data1
            - name: lakefs-data-2
              mountPath: /data2
      volumes:
        - name: lakefs-data-1
          persistentVolumeClaim:
            claimName: multi-mount-claim-1
        - name: lakefs-data-2
          persistentVolumeClaim:
            claimName: multi-mount-claim-2
    ```

=== "StatefulSet (Advanced)"

    Due to the nuances of how StatefulSets manage PersistentVolumeClaims, it is often simpler to use a `Deployment`.

    *   **Deletion:** When you delete a StatefulSet, its PVCs are not automatically deleted. You must delete them manually.
    *   **Replicas > 1:** Using more than one replica requires manually creating a corresponding number of `PersistentVolume` resources, as static provisioning does not automatically create them.

    ```yaml
    apiVersion: v1
    kind: PersistentVolume
    metadata:
      name: sts-simple-mount
      labels:
        app: sts-app-simple-everest
    spec:
      capacity:
        storage: 100Gi # ignored, required
      accessModes:
        - ReadOnlyMany
      csi:
        driver: csi.everest.lakefs.io
        volumeHandle: everest-csi-driver-volume-5 # Must be unique
        volumeAttributes:
          lakeFSMountUri: <LAKEFS_MOUNT_URI>
    ---
    apiVersion: apps/v1
    kind: StatefulSet
    metadata:
      name: sts-app-simple-everest
    spec:
      replicas: 1
      selector:
        matchLabels:
          app: sts-app-simple-everest
      template:
        metadata:
          labels:
            app: sts-app-simple-everest
        spec:
          containers:
            - name: app
              image: centos
              command: ["/bin/sh", "-c", "ls /data/; tail -f /dev/null"]
              volumeMounts:
                - name: sts-simple-mount
                  mountPath: /data
      volumeClaimTemplates:
      - metadata:
          name: sts-simple-mount
        spec:
          selector:
            matchLabels:
              app: sts-app-simple-everest
          storageClassName: "" # required for static provisioning
          accessModes: [ "ReadOnlyMany" ]
          resources:
            requests:
              storage: 5Gi # ignored, required
    ```

=== "Mount Options"

    This example demonstrates how to pass various `everest mount` flags via `mountOptions` in the `PersistentVolume` spec.

    ```yaml
    apiVersion: v1
    kind: PersistentVolume
    metadata:
      name: options-demo-pv
    spec:
      capacity:
        storage: 100Gi # ignored, required
      accessModes:
        - ReadOnlyMany
      # everest mount flags are passed here
      mountOptions:
        # set cache size in bytes
        - cache-size 10000000
        # set log level to trace for debugging (very noisy!)
        - log-level trace
        # WARN: Overriding credentials should only be used in advanced cases.
        # It is more secure to rely on the default credentials configured in the CSI driver.
        - lakectl-access-key-id <LAKEFS_ACCESS_KEY_ID>
        - lakectl-secret-access-key <LAKEFS_SECRET_ACCESS_KEY>
        - lakectl-server-url <LAKEFS_ENDPOINT>
    csi:
      driver: csi.everest.lakefs.io
      volumeHandle: everest-csi-driver-volume-6 # Must be unique
      volumeAttributes:
        lakeFSMountUri: <LAKEFS_MOUNT_URI>
    ---
    # PVC and Pod definitions follow...
    ```

### 4. Troubleshooting

*   Check logs from the CSI driver pods and the application pod that failed to mount.
*   Inspect the events and status of the `PV` and `PVC` (`kubectl get pv`, `kubectl get pvc`, `kubectl describe ...`).
*   **Advanced: SSH into the Kubernetes node** to inspect the `systemd` service logs for the specific mount operation.
    1.  Find the failed mount service:
        ```sh
        systemctl list-units --type=service | grep everest-lakefs-mount
        # Example output:
        # everest-lakefs-mount-0.0.8-everest-123.service loaded active running CSI driver FUSE daemon
        ```
    2.  Get the status and view the exact command that was executed:
        ```sh
        systemctl status everest-lakefs-mount-0.0.8-everest-123.service
        ```
    3.  View the logs for the service:
        ```sh
        journalctl -f -u everest-lakefs-mount-0.0.8-everest-123.service
        ```

---

## Reference

### Command-Line Interface

#### `everest mount`
Mounts a lakeFS URI to a local directory.

```bash
everest mount <lakefs_uri> <mount_directory> [flags]
```

**Tips:**
*   Since the server runs in the background, use `--log-output /path/to/file` to view logs.
*   The optimal cache size is the size of the data you are going to read/write.
*   To reuse the cache between restarts of the same mount, set the `--cache-dir` flag.
*   In read-only mode, if you provide a branch or tag, Everest will resolve and mount the HEAD commit. For a stable mount, use a specific commit ID in the URI.

**Flags:**
*   `--write-mode`: Enable write mode (default: `false`).
*   `--cache-dir`: Directory to cache files.
*   `--cache-size`: Size of the local cache in bytes.
*   `--cache-create-provided-dir`: If `cache-dir` is provided and does not exist, create it.
*   `--listen`: Address for the mount server to listen on.
*   `--no-spawn`: Do not spawn a new server; assume one is already running.
*   `--protocol`: Protocol to use (default: `nfs`).
*   `--log-level`: Set logging level.
*   `--log-format`: Set logging output format.
*   `--log-output`: Set logging output(s).
*   `--presign`: Use pre-signed URLs for direct object store access (default: `true`).
*   `--partial-reads`: (Experimental) Fetch only the accessed parts of large files. This can be useful for streaming workloads or for applications handling file formats such as Parquet, m4a, zip, and tar that do not need to read the entire file.

#### `everest umount`
Unmounts a lakeFS directory.

```bash
everest umount <mount_directory>
```

#### `everest diff` (Write Mode Only)
Shows the difference between the local mount directory and the source branch.

```bash
everest diff [mount_directory]
```

#### `everest commit` (Write Mode Only)
Commits local changes to the source lakeFS branch. The new commit is merged to the original branch using a `source-wins` strategy. After the commit succeeds, the mounted directory's source commit is updated to the new HEAD of the branch.

!!! warning
    Writes to a mount directory during a commit operation may be lost.

```bash
everest commit [mount_directory] -m <commit_message>
```

#### `everest mount-server` (Advanced)
Starts the mount server without performing the OS-level mount. This is intended for advanced use cases where you want to manage the server process and the OS mount command separately.

```bash
everest mount-server <remote_mount_uri> [flags]
```

**Flags:**
*   `--cache-dir`: Directory to cache read files and metadata.
*   `--cache-create-provided-dir`: Create the cache directory if it does not exist.
*   `--listen`: Address to listen on.
*   `--protocol`: Protocol to use (nfs | webdav).
*   `--callback-addr`: Callback address to report back to.
*   `--log-level`: Set logging level.
*   `--log-format`: Set logging output format.
*   `--log-output`: Set logging output(s).
*   `--cache-size`: Size of the local cache in bytes.
*   `--parallelism`: Number of parallel downloads for metadata.
*   `--presign`: Use presign for downloading.
*   `--write-mode`: Enable write mode (default: false).

### Consistency & File System Behavior

*   **Read-After-Write Consistency:** Within a single mount point, a write operation is guaranteed to be available for subsequent reads.
*   **lakeFS Consistency:** Local changes are only visible in lakeFS after a `commit`. Other users or mounts will not see changes until they are committed to the branch.
*   **Sync Operation:** The `commit` and `diff` commands first perform a `sync` operation to upload local changes to a temporary location in lakeFS for processing.

#### Write Mode Limitations

*   **Unsupported Operations:** `rename`, temporary files, hard/symbolic links, POSIX file locks, and POSIX permissions are not supported.
*   **Metadata:** Modifying file metadata (`chmod`, `chown`) is a no-op.
*   **Deletion:** A file's name cannot be reused as a directory after deletion, and vice-versa.

### Integration with Git

It is safe to mount a lakeFS path inside a Git repository. Everest automatically creates a virtual `.gitignore` file in the mount directory. This file instructs Git to ignore all mounted content *except* for a single file: `.everest/source`.

By committing the `.everest/source` file, which contains the `lakefs://` URI, you ensure that anyone who clones your Git repository and uses Everest will mount the exact same version of the data, making your project fully reproducible.

---

## FAQ

<h3>How does data access work? Does it stream through the lakeFS server?</h3>
No. By default (`--presign=true`), Everest uses pre-signed URLs to read and write data directly to and from the underlying object store, ensuring high performance. Metadata operations still go through the lakeFS server.

<h3>What happens if the lakeFS branch is updated after I mount it?</h3>
In read-only mode, your mount points to the commit that was at the HEAD of the branch *at the time of mounting*. It will not reflect subsequent commits to that branch unless you unmount and remount. In write mode, after a successful `commit`, the mount is updated to the new HEAD of the branch.

<h3>When are files downloaded?</h3>
Everest uses a lazy fetching strategy. Files are only downloaded when their content is accessed (e.g., with `cat`, `open`, or reading in a script). Metadata-only operations like `ls` do not trigger downloads.

<h3>What are the RBAC permissions required for mounting?</h3>
You can use lakeFS's [Role-Based Access Control](../security/rbac.md) to manage access.

**Minimal Read-Only Permissions:**
```json
{
  "id": "MountReadOnlyPolicy",
  "statement": [
    {
      "action": ["fs:ReadObject"],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repo>/object/<prefix>/*"
    },
    {
      "action": ["fs:ListObjects", "fs:ReadCommit", "fs:ReadBranch", "fs:ReadTag", "fs:ReadRepository"],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repo>"
    },
    { "action": ["fs:ReadConfig"], "effect": "allow", "resource": "*" }
  ]
}
```

**Minimal Write-Mode Permissions:**
```json
{
  "id": "MountWritePolicy",
  "statement": [
    {
      "action": ["fs:ReadObject", "fs:WriteObject", "fs:DeleteObject"],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repo>/object/<prefix>/*"
    },
    {
      "action": [
        "fs:ListObjects", "fs:ReadCommit", "fs:ReadBranch", "fs:ReadRepository",
        "fs:CreateCommit", "fs:CreateBranch", "fs:DeleteBranch", "fs:RevertBranch"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repo>"
    },
    { "action": ["fs:ReadConfig"], "effect": "allow", "resource": "*" }
  ]
}
```

<h3>Why use lakeFS Mount instead of `lakectl local`?</h3>
While both tools work with local data, they serve different needs. Use `lakectl local` for Git-like workflows where you need to pull and push entire directories. Use **lakeFS Mount** for cases where you want immediate, on-demand access to a large repository without downloading it first, making it ideal for exploration, training ML models, or any task that benefits from lazy loading.