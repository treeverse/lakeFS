---
title: Mount (Everest)
description: This section covers the Everest feature for mounting a lakeFS path to your local filesystem.
status: enterprise
---

# Mount (Everest)

!!! info
    Available in **lakeFS Cloud** and **lakeFS Enterprise**

Everest is a complementary binary to lakeFS that allows users to virtually mount a remote lakeFS repository onto a local directory.
Once mounted, users can access the data as if it resides on their local filesystem, using any tool, library, or framework that reads from a local filesystem.

!!! note
    No installation is required. Please [contact us](http://info.lakefs.io/thanks-lakefs-mounts) to get access to the Everest binary.

!!! tip
    Everest mount supports writing to the file system for both NFS and FUSE protocols starting version **0.2.0**!
    
    [Everest mount write mode semantics →](mount-write-mode-semantics.md).


<iframe width="420" height="315" src="https://www.youtube.com/embed/BgKuoa8LAaU"></iframe>

## Use Cases

* **Simplified Data Loading**: With lakeFS Mount, there's no need to write custom data loaders or use special SDKs. You can use your existing tools to read and write files directly from the filesystem.
* **Handle Large-scale Data Without changing Work Habits**: Seamlessly scale from a few local files to millions without changing your tools or workflow. Use the same code from early experimentation all the way to production.
* **Enhanced Data Loading Efficiency**: lakeFS Mount supports billions of files and offers fast data fetching, making it ideal for optimizing GPU utilization and other performance-sensitive tasks.

## Requirements

- For enterprise installations: lakeFS Version `1.25.0` or higher.

### OS and Protocol Support

Currently, the implemented protocols are `nfs` and `fuse`.

- NFS V3 (Network File System) is supported on macOS.

## Authentication Chain for lakeFS

When running an Everest `mount` command, authentication occurs in the following order:

1. **Session token** from the environment variable `EVEREST_LAKEFS_CREDENTIALS_SESSION_TOKEN` or `LAKECTL_CREDENTIALS_SESSION_TOKEN`.  
   If the token is expired, authentication will fail.
2. **lakeFS key pair**, using lakeFS access key ID and secret key. (picked up from lakectl if Everest not provided)
3. **IAM authentication**, if configured and **no static credentials are set**.

## Authenticate with lakeFS Credentials

The authentication with the target lakeFS server is equal to [lakectl CLI][lakectl].
Searching for lakeFS credentials and server endpoint in the following order:

- Command line flags `--lakectl-access-key-id`, `--lakectl-secret-access-key` and `--lakectl-server-url`
- `LAKECTL_*` Environment variables
- `~/.lakectl.yaml` Configuration file or via `--lakectl-config` flag

## Authenticating with AWS IAM Role

Starting from **lakeFS ≥ v1.57.0** and **Everest ≥ v0.4.0**, authenticating with IAM roles is supported!  
When IAM authentication is configured, Everest will use AWS SDK default behavior that will pick your **AWS environment** to generate a **session token** used for authenticating against lakeFS (i.e use `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, etc). This token is seamlessly refreshed as long as the AWS session remains valid.  

### Prerequisites

1. Make sure your lakeFS server supports [AWS IAM Role Login](../security/external-principals-aws.md).
2. Make sure your IAM role is attached to lakeFS. See [Administration of IAM Roles in lakeFS](../security/external-principals-aws.md#administration-of-iam-roles-in-lakefs)

### Configure everest to use IAM

To use IAM authentication, new configuration fields were introduced:

- `credentials.provider.type` `(string: '')` - Settings this `aws_iam` will expect `aws_iam` block and try to use IAM.
- `credentials.provider.aws_iam.token_ttl_second` `(duration: 60m)` - Optional: lakeFS token duration.
- `credentials.provider.aws_iam.url_presign_ttl_seconds` `(duration: 15m)` - Optional: AWS STS's presigned URL validation duration.  
- `credentials.provider.aws_iam.refresh_interval` `(duration: 15m)` - Optional: Amount of time before token expiration that Everest will try to fetch a new session token instead of using the current one.  
- `credentials.provider.aws_iam.token_request_headers`: Map of required headers and their values to be signed by the AWS STS request as configured in your lakeFS server. If nothing is set the **default** behavior is adding `x-lakefs-server-id:<lakeFS host>`. If your lakeFS server doesn't require any headers (less secure) you can set this empty by setting `{}` empty map in your config. 

These configuration fields can be set via `.lakectl.yaml`: 

!!! example
    ```yaml
    credentials:
    provider:
        type: aws_iam          # Required
        aws_iam:
        token_ttl_seconds: 60m              # Optional, default: 1h
        url_presign_ttl_seconds: 15m        # Optional, default: 15m
        refresh_interval: 5m                # Optional, default: 5m
        token_request_headers:              # Optional, if omitted then will set x-lakefs-server-id: <lakeFS host> by default, to override default set to '{}'
        # x-lakefs-server-id: <lakeFS host>     Added by default if token_request_headers is not set	
        custome-key:  custome-val
    server:
    endpoint_url: <lakeFS endpoint url>
    ```

To set using environment variables - those will start with the prefix `EVEREST_LAKEFS_*` or `LAKECTL_*`.
For example, setting the provider type using env vars:
`export EVEREST_LAKEFS_CREDENTIALS_PROVIDER_TYPE=aws_iam` or `LAKECTL_CREDENTIALS_PROVIDER_TYPE=aws_iam`.

!!! tip
    To troubleshoot presign request issues, you can enable debug logging for presign requests using the environment variable:
    
    ```bash
    EVEREST_LAKEFS_CREDENTIALS_PROVIDER_AWS_IAM_CLIENT_LOG_PRE_SIGNING_REQUEST=true
    ```

!!! warning
    If you choose to configure IAM provider using the same lakectl file (i.e `lakectl.yaml`) that you use for the **lakectl cli**, 
    you must upgrade lakectl to version (`≥ v1.57.0`) otherwise lakectl will raise errors when using it.


## Command Line Interface

### Mount Command

The `mount` command is used to mount a lakeFS repository to a local directory, it does it in 2 steps:

1. Starting a server that listens on a local address and serves the data from the remote lakeFS repository.
2. Running the required mount command on the OS level to connect the server to the local directory.

#### Tips:

- Since the server runs in the background set `--log-output /some/file` to view the logs in a file.
- Cache: Everest uses a local cache to store the data and metadata of the lakeFS repository. The optimal cache size is the size of the data you are going to read/write.
- Reusing Cache: between restarts of the same mount endpoint, set `--cache-dir` to make sure the cache is reused.
- Mounted data consistency (read-mode): When providing lakeFS URI mount endpoint `lakefs://<repo>/<ref>/<path>` the `<ref>` should be a specific commit ID. If a branch/tag is provided, Everest will use the HEAD commit instead.
- When running mount in write-mode, the lakeFS URI must be a branch name, not a commit ID or a tag.

#### Usage

```bash
everest mount <lakefs_uri> <mount_directory>

Flags
--presign: Use presign for downloading.
--cache-dir: Directory to cache read files in.
--cache-size: Size of the local cache in bytes.
--cache-create-provided-dir: If cache-dir is explicitly provided and does not exist, create it.
--listen: Address to listen on.
--no-spawn: Do not spawn a new server, assume one is already running.
--protocol: Protocol to use (default: nfs).
--log-level: Set logging level.
--log-format: Set logging output format.
--log-output: Set logging output(s).
--write-mode: Enable write mode (default: false).
```

### Umount Command

The `umount` command is used to unmount a currently mounted lakeFS repository.

```bash
everest umount <mount_directory>
```

### Diff Command (write-mode only)

The `diff` command Show the diff between the source branch and the current mount directory. 
If `<mount_directory>` not specified, the command searches for the mount directory in the current working directory and upwards based on `.everest` directory existence.
Please note that the diffs are from the source branch state at the time of mounting and not the current state of the source branch, i.e., changes to the source branch from other operations will not be reflected in the diff result.

```bash
everest diff <mount_directory>

#Example output:
# - removed datasets/pets/cats/persian/cute.jpg
# ~ modified datasets/pets/dogs/golden_retrievers/cute.jpg
# + added datasets/pets/birds/parrot/cute.jpg
```

### Commit Command (write-mode only)

The `commit` command commits the changes made in the mounted directory to the original lakeFS branch.
If `<mount_directory>` not specified, the command searches for the mount directory in the current working directory and upwards based on `.everest` directory existence.
The new commit will be merged to the original branch with the `source-wins` strategy.
After the commit is successful, the mounted directory source commit will be updated to the HEAD of the latest commit at the source branch; that means that changes made to the source branch out of the mount scope will also be reflected in the mounted directory.

!!! warning
    Writes to a mount directory during commit may be lost.

```bash
everest commit <mount_directory> -m <optional_commit_message>
```

### mount-server Command (Advanced)

!!! note
    The `mount-server` command is for advanced use cases and will only spin the server without calling OS mount command.

The mount-server command starts a mount server manually. Generally, users would use the mount command which handles server operations automatically.

```bash
everest mount-server <remote_mount_uri>
Flags
--cache-dir: Directory to cache read files and metadata.
--cache-create-provided-dir: Create the cache directory if it does not exist.
--listen: Address to listen on.
--protocol: Protocol to use (nfs | webdav).
--callback-addr: Callback address to report back to.
--log-level: Set logging level.
--log-format: Set logging output format.
--log-output: Set logging output(s).
--cache-size: Size of the local cache in bytes.
--parallelism: Number of parallel downloads for metadata.
--presign: Use presign for downloading.
--write-mode: Enable write mode (default: false).
```

### Partial Reads

!!! warning "Experimental"

When reading large files, Everest can fetch from lakeFS only the parts actually accessed.
This can be useful for streaming workloads or for applications handling file formats such as Parquet, m4a, zip, tar that do not need to read the entire file.

To enable partial reads, pass the `--partial-reads` flag to the `mount` (or `mount-server`) command:

```bash
everest mount --partial-reads "lakefs://image-repo/main/datasets/pets/" "./pets"
```

## Examples

### Read-Only Mode (default)

!!! info
    For simplicity, the examples show `main` as the ref, Everest will always mount a specific commit ID when using read-only mode, given a ref it will use the HEAD (e.g the most recent commit).

!!! example "Data Exploration"
    Mount the lakeFS repository and explore data as if it's on the local filesystem.

    ```bash
    everest mount "lakefs://image-repo/main/datasets/pets/" "./pets"
    ls -l "./pets/dogs/"
    find ./pets -name "*.small.jpg"
    open -a Preview "./pets/dogs/golden_retrievers/cute.jpg"
    everest umount "./pets"
    ```

!!! example "Working with Data Locally"
    Mount the remote lakeFS server and use all familiar tools without changing the workflow.

    ```bash
    everest mount lakefs://image-repo/main/datasets/pets/ ./pets
    pytorch_train.py --input ./pets
    duckdb "SELECT * FROM read_parquet('pets/labels.parquet')"
    everest umount ./pets
    ```

### Write Mode

!!! example "Changing Data Locally"
    Mount the remote lakeFS server in write mode and change data locally.

    ```bash
    everest mount lakefs://image-repo/main/datasets/pets/ ./pets --write-mode
    # Add a new file
    echo "new data" >> ./pets/birds/parrot/cute.jpg
    # Update an existing file
    echo "new data" >> ./pets/dogs/golden_retrievers/cute.jpg
    # Delete a file
    rm ./pets/cats/persian/cute.jpg

    # Check the changes
    everest diff ./pets
    # - removed datasets/pets/cats/persian/cute.jpg
    # ~ modified datasets/pets/dogs/golden_retrievers/cute.jpg
    # + added datasets/pets/birds/parrot/cute.jpg

    # Commit the changes to the original lakeFS branch
    everest commit ./pets

    everest diff ./pets
    # No changes

    everest umount ./pets
    ```

To learn more, read about [Mount Write Mode Semantics](../reference/mount-write-mode-semantics.md).


[lakectl]: ./cli.md

## Mount CSI Driver (Everest on Kubernetes)

!!! warning "Private Preview"
    The CSI Driver is in private preview. Please [contact us](http://info.lakefs.io/thanks-lakefs-mounts) to get access.

The lakeFS CSI (Container Storage Interface) Driver is an extension for Kubernetes that enables seamless access to data within a lakeFS repository, allowing Pods to interact with lakeFS data as if it were part of the local filesystem. This driver builds on the functionality of [Everest](./mount.md), which provides a read-only view of lakeFS data by virtually mounting a repository.

### How mount is executed on a Host

- While the `csi` driver is responsible for mounting and unmounting the volume on the host, it does not need permissions to execute the `mount` and `umount` commands directly.
- The `everest` commands are executed by `systemd` service on the Host itself (i.e `everest mount...`). 
- The `csi` driver communicates with the `systemd` service via a unix socket to execute the `mount` and `umount` commands.

### Status and Limitations

- Tested OS: BottleRocket-OS, Amazon Linux 2 and RHEL 8.
- Minimal Kubernetes versions `>=1.23.0`. 
- Tested Cluster providers EKS, Openshift (Partially).
- Static provisioning only explain below.
- Setting Pods `securityContext` UID and GID (i.e `runAsUser: 1000`, `runAsGroup: 2000`) is very nuanced in nature and does not have wide coverage currently, not supported but might work.
- Pod only supports access mode `ReadOnlyMany`

**Static Provisioning only (Relevant for pods)**

When requesting a mount from the CSI driver, the driver will create a `PersistentVolume` (PV) and `PersistentVolumeClaim` (PVC) for the Pod.
The driver only supports Static Provisioning as of today, and you need an existing lakeFS repository to use.

To use Static Provisioning, you should set `storageClassName` field of your `PersistentVolume (PV)` and `PersistentVolumeClaim (PVC)` to `""` (empty string). Also, in order to make sure no other PVCs can claim your PV, you should define a one-to-one mapping using `claimRef`.

### Requirements

1. For enterprise installations: lakeFS Version `1.25.0` or higher.
2. You have a Kubernetes cluster with version `>=1.23.0` and [Helm](https://helm.sh/docs/intro/install/) installed. 
3. lakeFS Server that can be access from pods in the cluster.
4. Access to download *treeverse/everest-lakefs-csi-driver* from [Docker Hub](https://hub.docker.com/u/treeverse). [Contact us](https://lakefs.io/contact-sales/) to gain access to lakeFS Enterprise features.

### Deploy the CSI Driver 

The CSI Driver is deployed to K8S cluster using a dedicated Helm chart [everest-lakefs-csi-driver](https://github.com/treeverse/charts/releases).

#### 1. Update your helm with the chart:

Add lakeFS Helm repository if not already added:

```bash
helm repo add lakefs https://charts.lakefs.io
```

Fetch the chart from lakeFS repository:

```bash
helm repo update lakefs
```

Verify the chart is available and updated: 

```bash
helm show chart lakefs/everest-lakefs-csi-driver 
```

List all available chart versions:

```bash
helm search repo lakefs/everest-lakefs-csi-driver -l
```

#### 2. Configure the values for the CSI Driver in a `values.yaml` file

**Helm Chart default values:**

```bash
helm show values lakefs/everest-lakefs-csi-driver --version <version>
```

**CSI driver config:**

All the driver CLI flags can be configured via environment variables (prefixed `CSI_DRIVER_`) and can be passed to the driver.

!!! example "Example: Minimal required arguments not commented"

    ```yaml
    # image:  
    #   repository: treeverse/everest-lakefs-csi-driver
    # # Optional CSI Driver override version (default .Chart.AppVersion)
    #   tag: 1.2.3

    # Same as fluffy https://github.com/treeverse/fluffy?tab=readme-ov-file#1-dockerhub-token-for-fluffy
    imagePullSecret:
    token: <dockerhub-token>
    username: <dockerhub-user>

    # Credentials that will be used by everest as a default to access lakeFS mount paths
    lakeFSAccessSecret:
    keyId: <lakefs-key-id>
    accessKey: <lakefs-access-key>
    endpoint: <lakefs-endpoint>

    node:
    # verbosity level of the driver (normal values are 0-4, 5 would be most verbose)
    logLevel: 4
    # Only set if having issues with running or installing the everest binary 
    # Path directory where the everest binary accessed by the underlying K8S Nodes (${everestInstallPath}/everest)
    # The binary will copied from the CSI pod into that location by the init container job in the node.yaml
    # This path will be a host path on the K8S Nodes
    # depending on the underlying OS and the SELinux policy the binary will be executed by systemd on the Host.
    # Known issue when using Bottlerocket OS https://github.com/bottlerocket-os/bottlerocket/pull/3779 
    # everestInstallPath: /opt/everest-mount/bin/  # should end with "/"

    # Additional environment variables that will be passed to the driver can be used to configure the csi driver
    # extraEnvVars:
    #   - name: CSI_DRIVER_MOUNT_TIMEOUT
    #     value: "30s"
    #   - name: CSI_DRIVER_EVEREST_DEFAULT_CACHE_SIZE
    #     value: "10000000000"
    #   - name: VALUE_FROM_SECRET
    #     valueFrom:
    #       secretKeyRef:
    #         name: secret_name
    #         key: secret_key
    ```

#### 3. Install the Chart to K8S cluster

Install the chart with the values file:

```bash     
helm install -f values.yaml lakefs lakefs/everest-lakefs-csi-driver --version <version>
```

### Use in Pods

Once the CSI Driver is installed, we can start using it similarly to how all `PersistentVolume` (PV) and `PersistentVolumeClaim` (PVC) are used in Kubernetes.

The only required argument to set is `lakeFSMountUri` in the `PV` (See examples below).

#### Mount Options

Most of the options are optional and can be omitted, but each mount request can be configured with [everest mount cli options](./mount.md#mount-command), they are passed as `mountOptions` in the `PVC` spec.

#### Examples

The examples demonstrates different mount scenarios with the CSI Driver.
All of them are essentially running `ls <mount-dir>` and `tail -f /dev/null` in a centos container.
If the mount succeeded you will see the contents of your mount directory.

1. Set `lakeFSMountUri` (i.e `lakefs://<repo>/<repo>/[prefix/]`) to the lakeFS mount URI you want to mount.
1. Run `kubectl apply -f values.yaml`
1. View the example pod logs to see the mount output `kubectl logs -f <pod-name>`
  

=== "Single Pod and mount"

    Configure `lakeFSMountUri` to the target URI. 

    ```yaml
    apiVersion: v1
    kind: PersistentVolume
    metadata:
    name: everest-pv
    spec:
    capacity:
        storage: 100Gi # ignored, required
    accessModes:
        - ReadOnlyMany # supported options: ReadOnlyMany
    # everest mount options goes under mountOptions and forwarded to the everest mount command 
    # mountOptions:
        # set cache size in bytes 
        # - cache-size 1000000000
    csi:
        driver: csi.everest.lakefs.io # required
        volumeHandle: everest-csi-driver-volume
        volumeAttributes:
        # mount target, replace with your lakeFS mount URI
        lakeFSMountUri: <LAKEFS_MOUNT_URI>

    ---
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
    name: everest-claim
    spec:
    accessModes:
        - ReadOnlyMany # supported options: ReadOnlyMany
    storageClassName: "" # required for static provisioning
    resources:
        requests:
        storage: 5Gi # ignored, required
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
        command: ["/bin/sh"]
        args: ["-c", "ls /data/; tail -f /dev/null"]
        volumeMounts:
            - name: persistent-storage-isan
            mountPath: /data
    volumes:
        - name: persistent-storage-isan
        persistentVolumeClaim:
            claimName: everest-claim

    ```

=== "Multiple Pods, one mount (Deployment)"
    Configure `lakeFSMountUri` to the target URI.

    In this example a deployment is created with 3 replicas, all sharing a single `PersistentVolume` and PVC 
    Behind the scenes each pod get's their own mount, even if on the same k8s node, each pod will get their own mount directory.

    Unlike in StatefulSet, this can scale-up-down with no additional interference and deleted easily the same way.

    ```yaml
    apiVersion: v1
    kind: PersistentVolume
    metadata:
    name: multiple-pods-one-pv
    spec:
    capacity:
        storage: 1200Gi # ignored, required
    accessModes:
        - ReadOnlyMany # supported options: ReadOnlyMany
    # everest mount options goes under mountOptions and forwarded to the everest mount command 
    # mountOptions:
    #   - cache-size 1000000555
    csi:
        driver: csi.everest.lakefs.io # required
        volumeHandle: everest-csi-driver-volume
        volumeAttributes:
        # mount target, replace with your lakeFS mount URI
        lakeFSMountUri: <LAKEFS_MOUNT_URI>
    ---
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
    name: multiple-pods-one-claim
    spec:
    accessModes:
        - ReadOnlyMany # supported options: ReadOnlyMany
    storageClassName: "" # required for static provisioning
    resources:
        requests:
        storage: 1200Gi # ignored, required
    volumeName: multiple-pods-one-pv
    ---
    apiVersion: apps/v1
    kind: Deployment
    metadata:
    name: multiple-pods-one-pv-app
    labels:
        app: multiple-pods-one-pv-app
    spec:
    replicas: 3
    selector:
        matchLabels:
        app: multiple-pods-one-pv-app
    template:
        metadata:
        labels:
            app: multiple-pods-one-pv-app
        spec:
        containers:
        - name: multiple-pods-one-pv-app
            image: centos
            command: ["/bin/sh"]
            args: ["-c", "ls /data/; tail -f /dev/null"]
            volumeMounts:
            - name: persistent-storage
            mountPath: /data
            ports:
            - containerPort: 80
        volumes:
        - name: persistent-storage
            persistentVolumeClaim:
            claimName: multiple-pods-one-claim

    ```


=== "Multiple mounts, single Pod"

    Deploy a pod with two mounts to different mount points.
    Configure `lakeFSMountUri` for each `PersistentVolume`. 

    ```yaml
    apiVersion: v1
    kind: PersistentVolume
    metadata:
    name: multiple-mounts-one-pod-pv
    spec:
    capacity:
        storage: 1200Gi # ignored, required
    accessModes:
        - ReadOnlyMany # supported options: ReadOnlyMany
    mountOptions:
        - cache-size 1000000111
    csi:
        driver: csi.everest.lakefs.io # required
        volumeHandle: everest-csi-driver-volume # must be unique
        volumeAttributes:
        # mount target local-lakefs dir, replace with your lakeFS mount URI
        lakeFSMountUri: <LAKEFS_URI_1>
    ---
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
    name: multple-mounts-one-pod-claim
    spec:
    accessModes:
        - ReadOnlyMany # supported options: ReadOnlyMany
    storageClassName: "" # required for static provisioning
    resources:
        requests:
        storage: 1200Gi # ignored, required
    volumeName: multiple-mounts-one-pod-pv
    ---
    apiVersion: v1
    kind: PersistentVolume
    metadata:
    name: multiple-mounts-one-pod-pv-2
    spec:
    capacity:
        storage: 1200Gi # ignored, required
    accessModes:
        - ReadOnlyMany # ReadOnlyMany
    mountOptions:
        - cache-size 1000000555
    csi:
        driver: csi.everest.lakefs.io # required
        volumeHandle: everest-csi-driver-volume-2 # must be unique
        volumeAttributes:
        # mount target images dir, replace with your lakeFS mount URI
        lakeFSMountUri: <LAKEFS_URI_2>
    ---
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
    name: multple-mounts-one-pod-claim-2
    spec:
    accessModes:
        - ReadOnlyMany # supported options: ReadOnlyMany
    storageClassName: "" # required for static provisioning
    resources:
        requests:
        storage: 1200Gi # ignored, required
    volumeName: multiple-mounts-one-pod-pv-2
    ---
    apiVersion: v1
    kind: Pod
    metadata:
    name: everest-multi-mounts-one-pod
    spec:
    containers:
        - name: app
        image: centos
        command: ["/bin/sh"]
        args: ["-c", "ls /data/; ls /data2/; tail -f /dev/null"]
        volumeMounts:
            - name: persistent-storage
            mountPath: /data
            - name: persistent-storage-2
            mountPath: /data2
    volumes:
        - name: persistent-storage
        persistentVolumeClaim:
            claimName: multple-mounts-one-pod-claim
        - name: persistent-storage-2
        persistentVolumeClaim:
            claimName: multple-mounts-one-pod-claim-2

    ```

=== "StatefulSet (Advanced)"

    Configure `lakeFSMountUri` to the target URI.
    Because of the neuances described below, if not required it is best to avoid using a `StatefulSet`.

    **Deletion:**

    It's [intended behavior](https://kubernetes.io/docs/tasks/run-application/delete-stateful-set/#complete-deletion-of-a-statefulset) for StatefulSet in K8S that the PVC is not deleted automatically when the pod is deleted since the StatefulSet controller does not manage the PVC.
    To completley delete use k delete with --force flag or first delete the PVC: 'kubectl delete pvc -l app=sts-app-simple-everest'

    **Scale Down:**

    replicas: 0 can be set to scale down the StatefulSet and bring back up with replicas: 1.

    **Replicas > 1:**

    not supported in this example, since the driver only supports static provisioning. 
    to use Statefulset with replica > 1 we need to add PersistentVolume(s) manually.

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
        - ReadOnlyMany # supported options: ReadOnlyMany
    mountOptions:
        # override default cache size for the mount (in bytes)
        - cache-size 1000000555
        - log-level debug
    csi:
        driver: csi.everest.lakefs.io # required
        volumeHandle: everest-csi-driver-volume
        volumeAttributes:
        # mount target, replace with your lakeFS mount URI
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
            command: ["/bin/sh"]
            args: ["-c", "ls /data/; tail -f /dev/null"]
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

    This demonstrates common flags and uncommon flags that can be used for a mount.
    In general, the flags are set in `mountOptions` and are passed to the everest [mount command](https://docs.lakefs.io/reference/mount.html#mount-command).

    ```yaml
    apiVersion: v1
    kind: PersistentVolume
    metadata:
    name: options-demo-pv
    spec:
    capacity:
        storage: 100Gi # ignored, required
    accessModes:
        - ReadOnlyMany # supported options: ReadOnlyMany
    # everest mount options goes under mountOptions and forwarded to the everest mount command 
    mountOptions:
        # set cache size in bytes 
        - cache-size 10000000
        # set log level to debug when inspecting mount logs (very noisy!)
        - log-level trace
        # WARN: lakeFS credentials / endpoint should be managed securely by the CSI-driver, this is an advanced flag use-case
        # override default lakeFS credentials (for use-cases where the default csi-driver credentials are not sufficient)
        - lakectl-access-key-id <LAKEFS_ACCESS_KEY_ID>
        - lakectl-secret-access-key <LAKEFS_SECRET_ACCESS_KEY>
        - lakectl-server-url <LAKEFS_ENDPOINT>
        # WARN: an advanced flag and rarely needed if at all, performs mount directly using fuser relying on it to exist on the host server without using FUSE syscalls
        # be default fuse-direct-mount is true
        # - fuse-direct-mount false
        # - mount-gid 2000
        # - mount-uid 1000
        # - presign false
        # - log-enable-syslog false

    csi:
        driver: csi.everest.lakefs.io # required
        volumeHandle: everest-csi-driver-volume
        volumeAttributes:
        # mount target, staging org (non default credentials on csi), replace with your lakeFS mount URI
        lakeFSMountUri: <LAKEFS_MOUNT_URI>

    # REST OF THE RESOURCES
    # ... 
    ```


### Troubleshooting

- Use `kubectl` and check the CSI driver pod and failed Pod for logs and events. 
- If a specific mount request failed, specifically inspect csi-node that the failed mount pod was deployed on. 
- Check the events and status of the `PVC` and `PV` of the failing pod `kubectl get pv && kubectl get pvc`

**Advanced: SSH into the underlying K8S node:**

Find the failed mount service `systemctl list-units --type=service`: 

```sh
everest-lakefs-mount-0.0.8-everest-123.service loaded active running CSI driver FUSE daemon
```

Get systemd service status:

```sh
# service name example: everest-lakefs-mount-0.0.8-everest-123.service
systemctl status <service>

# output contains many things including the exec command to run, example:
# ExecStart=/opt/bin/everest mount lakefs://test-mount/main/local-lakefs/ /var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/everest-pv/mount --log-level=trace --cache-dir=/var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/everest-pv/.everest --lakectl-config=/opt/mountpoint-s3-csi/bin/lakectl.yaml
```

See systemd logs of a service:

```sh
journalctl -f -u <service>

# example:
journalctl -f -u everest-lakefs-mount-0.0.8-everest-123.service
```


## FAQs

<h3>How do I get started with lakeFS Mount (Everest)?</h3>

lakeFS Mount is available for lakeFS Cloud and lakeFS Enterprise customers. Once your setup is complete, [contact us](http://info.lakefs.io/thanks-lakefs-mounts) to access the lakeFS Mounts binary and follow the provided docs.

* Want to try lakeFS Cloud? [Signup](https://lakefs.cloud/register) for a 30-day free trial.
* Interested in lakeFS Enterprise? [Contact sales](https://lakefs.io/contact-sales/) for a 30-day free license.

<h3>What operating systems are supported by lakeFS Mount?</h3>

lakeFS Mount supports Linux and MacOS. Windows support is on the roadmap.

<h3>How can I control access to my data when using lakeFS Mount?</h3>

You can use lakeFS's existing [Role-Based Access Control mechanism](../security/rbac.md), which includes repository and path-level policies. lakeFS Mount translates filesystem operations into lakeFS API operations and authorizes them based on these policies.

The minimal RBAC permissions required for mounting a prefix from a lakeFS repository in read-only mode:

```json
{
  "id": "MountPolicy",
  "statement": [
    {
      "action": [
        "fs:ReadObject"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repository-name>/object/<prefix>/*"
    },
    {
      "action": [
        "fs:ListObjects",
        "fs:ReadCommit",
        "fs:ReadBranch",
        "fs:ReadTag",
        "fs:ReadRepository"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repository-name>"
    },
    {
      "action": ["fs:ReadConfig"],
      "effect": "allow",
      "resource": "*"
    }
  ]
}
```

The minimal RBAC permissions required for mounting a prefix from a lakeFS repository in write mode:

```json
{
  "id": "MountPolicy",
  "statement": [
    {
      "action": [
        "fs:ReadObject",
        "fs:WriteObject",
        "fs:DeleteObject"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repository-name>/object/<prefix>/*"
    },
    {
      "action": [
        "fs:ListObjects",
        "fs:ReadCommit",
        "fs:ReadBranch",
        "fs:ReadRepository",
        "fs:CreateCommit",
        "fs:CreateBranch",
        "fs:DeleteBranch",
        "fs:RevertBranch"
      ],
      "effect": "allow",
      "resource": "arn:lakefs:fs:::repository/<repository-name>"
    },
    {
      "action": ["fs:ReadConfig"],
      "effect": "allow",
      "resource": "*"
    }
  ]
}
```

<h3>Does data pass through the lakeFS server when using lakeFS Mount?</h3>

lakeFS Mount leverages pre-signed URLs to read data directly from the underlying object store, meaning data doesn't  pass through the lakeFS server. By default, presign is enabled. To disable it, use:

```shell
everest mount <lakefs_uri> <mount_directory> --presign=false
```

<h3>What happens if a lakeFS branch is updated after I mount it?</h3>

lakeFS Mount points to the commit that was the HEAD commit of the branch at the time of mounting. This means the local directory reflects the branch state at the time of mounting and does not update with subsequent branch changes.

<h3>When are files downloaded to my local environment?</h3>

lakeFS Mount uses a lazy prefetch strategy. Files are not downloaded at mount time or during operations that only inspect file metadata (e.g., `ls`). Files are downloaded only when commands that require file access (e.g., `cat`) are used.

<h3>What are the scale limitations of lakeFS Mount, and what are the recommended configurations for dealing with large datasets?</h3>

When using lakeFS Mount, the volume of data accessed by the local machine influences the scale limitations more than the total size of the dataset under the mounted prefix. This is because lakeFS Mount uses a lazy downloading approach, meaning it only downloads the accessed files. lakeFS Mount listing capability is limited to performing efficiently for prefixes containing fewer than 8000 objects, but we are working to increase this limit.

<h5>Recommended Configuration</h5>

Ensure your **cache size** is large enough to accommodate the volume of files being accessed.

<h3>How does lakeFS Mount integrate with a Git repository?</h3>

It is perfectly safe to mount a lakeFS path within a Git repository.
lakeFS Mount prevents git from adding mounted objects to the git repository (i.e when running `git add -A`) by adding a virtual `.gitignore` file to the mounted directory.


The `.gitignore` file will also instruct Git to ignore all files except `.everest/source` and in its absence, it will try to find a `.everest/source` file in the destination folder, and read the lakeFS URI from there.
Since `.everest/source` is in source control, it will mount the same lakeFS commit every time!

<h3>I'm already using lakectl local for working with lakeFS data locally, why should I use lakeFS Mount?</h3>

While both lakectl local and lakeFS Mount enable working with lakeFS data locally, they serve different purposes:

<h5>Use lakectl local</h5>

* For enabling lakeFS writes with [lakectl local commit](../reference/cli.md#lakectl-local-commit).
* To integrate seamlessly with [Git](../integrations/git.md).

<h5>Use lakeFS Mount</h5>

For local data access, lakeFS Mount offers several benefits over lakectl local:

* **Optimized selective data access**: The lazy prefetch strategy saves storage space and reduces latency by only fetching the required data.
* **Reduced initial latency**: Start working on your data immediately without waiting for downloads.
