---
title: Mount CSI Driver
description: This section covers the usage of CSI Driver with Everest feature for mounting a lakeFS path to Kubernetes.
grand_parent: Reference
parent: Features
---

# Mount CSI Driver (Everest on Kubernetes)

{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

lakeFS Enterprise
{: .label .label-purple }

The lakeFS CSI (Container Storage Interface) Driver is an extension for Kubernetes that enables seamless access to data within a lakeFS repository, allowing Pods to interact with lakeFS data as if it were part of the local filesystem. This driver builds on the functionality of [Everest](./mount.md), which provides a read-only view of lakeFS data by virtually mounting a repository.

**Note**
⚠️ The CSI Driver is in private preview. Please [contact us](http://info.lakefs.io/thanks-lakefs-mounts) to get access.
{: .note }

{% include toc_2-3.html %}

## How mount is executed on a Host

- While the `csi` driver is responsible for mounting and unmounting the volume on the host, it does not need permissions to execute the `mount` and `umount` commands directly.
- The `everest` commands are executed by `systemd` service on the Host itself (i.e `everest mount...`). 
- The `csi` driver communicates with the `systemd` service via a unix socket to execute the `mount` and `umount` commands.

## Status and Limitations

- Tested OS: BottleRocket-OS, Amazon Linux 2 and RHEL 8.
- Minimal Kubernetes versions `>=1.23.0`. 
- Tested Cluster providers EKS, Openshift (Partially).
- Static provisioning only explain below.
- Setting Pods `securityContext` UID and GID (i.e `runAsUser: 1000`, `runAsGroup: 2000`) is very neuanced in nature and does not have wide coverage currently, not supported but might work.
- Pod only supports access mode `ReadOnlyMany`

**Static Provisioning only (Relevant for pods)**

When requesting a mount from the CSI driver, the driver will create a `PersistentVolume` (PV) and `PersistentVolumeClaim` (PVC) for the Pod.
The driver only supports Static Provisioning as of today, and you need an existing lakeFS repository to use.

To use Static Provisioning, you should set `storageClassName` field of your `PersistentVolume (PV)` and `PersistentVolumeClaim (PVC)` to `""` (empty string). Also, in order to make sure no other PVCs can claim your PV, you should define a one-to-one mapping using `claimRef`.

## Requirements

1. For enterprise installations: lakeFS Version `1.25.0` or higher.
2. You have a Kubernetes cluster with version `>=1.23.0` and [Helm](https://helm.sh/docs/intro/install/) installed. 
3. lakeFS Server that can be access from pods in the cluster.
4. Access to download *treeverse/everest-lakefs-csi-driver* from [Docker Hub](https://hub.docker.com/u/treeverse). [Contact us](https://lakefs.io/contact-sales/) to gain access to lakeFS Enterprise features.

## Deploy the CSI Driver 

The CSI Driver is deployed to K8S cluster using a dedicated Helm chart [everest-lakefs-csi-driver](https://github.com/treeverse/charts/releases).

### 1. Update your helm with the chart:

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

### 2. Configure the values for the CSI Driver in a `values.yaml` file

**Helm Chart default values:**

```bash
helm show values lakefs/everest-lakefs-csi-driver --version <version>
```

**CSI driver config:**

All the driver CLI flags can be configured via environment variables (prefiedx `CSI_DRIVER_`) and can be passed to the driver.

**Example `values.yaml` file - Minimal required arguments not commented:**

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

### 3. Install the Chart to K8S cluster

Install the chart with the values file:

```bash     
helm install -f values.yaml lakefs lakefs/everest-lakefs-csi-driver --version <version>
```

## Use in Pods

Once the CSI Driver is installed, we can start using it similarly to how all `PersistentVolume` (PV) and `PersistentVolumeClaim` (PVC) are used in Kubernetes.

The only required argument to set is `lakeFSMountUri` in the `PV` (See examples below).

### Mount Options

Most of the options are optional and can be omitted, but each mount request can be configured with [everest mount cli options](https://docs.lakefs.io/reference/mount.html#mount-command), they are passed as `mountOptions` in the `PVC` spec.

### Examples

The examples demonstrates different mount sncenarios with the CSI Driver.
All of them are essentially running `ls <mount-dir>` and `tail -f /dev/null` in a centos container.
If the mount succeeded you will see the contents of your mount directory.

1. Set `lakeFSMountUri` (i.e `lakefs://<repo>/<repo>/[prefix/]`) to the lakeFS mount URI you want to mount.
1. Run `kubectl apply -f values.yaml`
1. View the example pod logs to see the mount output `kubectl logs -f <pod-name>`
  
<div class="tabs">
  <ul>
    <li><a href="#k8s-simple-pod">Single Pod and mount</a></li>
    <li><a href="#k8s-deployment-multiple-pods-one-pv">Multiple Pods, one mount (Deployment)</a></li>
    <li><a href="#k8s-multiple-mounts-one-pod">Multiple mounts, single Pod</a></li>
    <li><a href="#k8s-simple-stateful-set">StatefulSet (Advanced)</a></li>
    <li><a href="#k8s-mount-options">Mount Options</a></li>
  </ul>

<div markdown="1" id="k8s-simple-pod">

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

</div> 
<div markdown="1" id="k8s-deployment-multiple-pods-one-pv">

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
</div> 
<div markdown="1" id="k8s-multiple-mounts-one-pod">

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

</div> 
<div markdown="1" id="k8s-simple-stateful-set">

Configure `lakeFSMountUri` to the target URI.
Because of the neuances described below, if not required it is best to avoid using a `StatefulSet`.

**Deletion:**

It's [intended behavior](https://kubernetes.io/docs/tasks/run-application/delete-stateful-set/#complete-deletion-of-a-statefulset) for StatefulSet in K8S that the PVC is not deleted automatically when the pod is deleted since the StatefulSet controller does not manage the PVC.
To completley delete use k delete with --force flag or first delete the PVC: 'kubectl delete pvc -l app=sts-app-simple-everest'

**Scale Down:**

replicas: 0 can be set to scale down the StatefulSet and bring back up with replicas: 1.

**Replicas > 1:**

not supported in this example, since the driver only supports static provisoning. 
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
</div> 

<div markdown="1" id="k8s-mount-options">

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
    # WARN: an advanced flag and rarelly needed if at all, performs mount directly using fuser relying on it to exist on the host server without using FUSE syscalls
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
</div>

</div>

## Troubleshooting

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

<!-- END EXCLUDE FROM TOC -->
