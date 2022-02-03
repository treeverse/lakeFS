# Kubernetes Job Hook Proposal

## Goals

- Allow lakeFS actions directly to trigger actions running on Kubernetes
- Enable non-blocking execution for post-merge and post-merge
- Support successful job completion as condition for the commit and merge operations


## Non Goals

- GitHub Actions - fully maintained, virtualized environment with pre-build images to execute pre/post hooks
- Manage or control any aspect of the Kubernetes Job


## Proposition


### Overview

Current hooks mechanism described [here](https://docs.lakefs.io/setup/hooks.html)

New lakeFS hook type, triggered by pre/post commit/merge events to execute jobs on k8s cluster.

The user will provide a hook definition that will include the name of the image (docker image) and arguments that will be used for the hook.
During commit/merge, lakeFS will schedule a job to run in the cluster. Successful response from the cluster for the job creation, will be considered successful and complete the commit/merge operation.


### New hook definition

Based on the current actions mechanism, the user will need to write a yaml file and upload it into the repository `_lakefs_actions` folder.

Example of an action using the new hook definition:

```yaml
name: Branch version tagger
description: set version tag on each merge to main
on:
  post-merge:
    branches:
      - main
hooks:
  - id: update_tag
    type: k8s-job
    description: Create a tag based on last version
    properties:
      image: "myregistry/myhook:4"
      command: ["python"]
      args: ["bump-version.py"]
      env:
      - name: REPOSITORY
        value: customers
      - name: PROJECT
        value: alpha
```

In this example we specified a post merge hook to execute a job. The job will use the user-supplied image `myregistry/myhook:4` with the command `python` using the argument `bump-version.py`.
The container will have the environment variables populated with REPOSITORY and PROJECT as defined by the user.

The following environment variables will be also populated with the event information:

```
LAKEFS_HOOK_EVENTTYPE - Type of the event that triggered the action
LAKEFS_HOOK_EVENTTIME - Time of the event that triggered
LAKEFS_HOOK_ACTIONNAME - Containing Hook Action's name
LAKEFS_HOOK_HOOKID - ID of the hook
LAKEFS_HOOK_REPOSITORYID - ID of the repository
LAKEFS_HOOK_BRANCHID - ID of the branch
LAKEFS_HOOK_SOURCEREF - Reference to the source that triggered the event
LAKEFS_HOOK_COMMITMESSAGE - The message for the commit
LAKEFS_HOOK_COMMITTER - Name of the committer
LAKEFS_HOOK_COMMIT_METADATA - Commit metadata (json serialized string)
```

By default lakeFS will use the following as base definition to schedule a job:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: ""
  namespace: lakefs-hooks
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: hook
        image: ""
        command: []
        args: []
```

Note that the metadata.name, image, command and args will be set by lakeFS.
Name - unique identifier to identify the specific job execution
Image, command and args - set based on the hook information


*Limit the end-user image use*

Using the lakeFS configuration, we can specify a list of allowed images that the end-user and use in the k8s-job hook:

```yaml
hooks:
  k8s-job:
    allowed_images:
      - rclone/rclone:1.57
      - alpine
```

Each item list will match the `registry/name:tag` used in the hook.
If the tag is omitted, any tag will be allowed.


### Execution

lakeFS requests job creation from the cluster.  Job information will be captured to the job's log.
In case the request fails it will log the error and fail the hook execution.
lakeFS will consider job creation as successful execution. Will log the job information without waiting for the job to complete or capturing the output.

### Authorizations

Base on the above, lakeFS will require the following permissions:

- `job` get, create and watch
- `pod` get
- `pod/log` get, list, watch

The following describes possible ClusterRole that enables the above. We can limit the scope to a single namespace using Role.
Note that we need to add the rules to the current set used by lakefs, this document describes the requirements for this feature.

```
apiVersion: v1
kind: ServiceAccount
metadata:
  namespace: default
  name: lakefs
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: lakefs
rules:
  - apiGroups: [""]
    resources: ["job"]
    verbs: ["get", "create", "watch"]
  - apiGroups: [""]
    resources: ["pod"]
    verbs: ["get"]
  - apiGroups: [""]
    resources: ["job/logs"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: lakefs
subjects:
  - kind: ServiceAccount
    namespace: default
    name: lakefs
    apiGroup: ""
roleRef:
  kind: ClusterRole
  name: lakefs
  apiGroup: rbac.authorization.k8s.io
```


### Considerations

*Job lifetime* - Once a job is created and executed in the cluster, the lakeFS server will not take ownership of the object. A mechanism should be in place to clean up all jobs lakeFS applied and completed (successfully or not).
[Automatic Clean-up for Finished Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/ttlafterfinished/) capability is currently found on Kubernetes 1.23 (which we don’t have yet on AWS for example) which can help with that.


