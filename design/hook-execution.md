# Hook Execution


### Overview

(from https://github.com/treeverse/lakeFS/issues/2231)

Currently, lakeFS supports web and airflow hooks. While they are very easy to understand, they are not always easy in terms of operations:

 - They require a server to be deployed and provide an uptime SLA
 - They require setting up network communication between said server and lakeFS
 - Depending on security protocols, they often require some sort of authentication
 - They make very simple things more complex than needed (i.e. a one-liner in bash now needs to be wrapped in a web application)
 - They make it hard to experiment locally, if lakeFS is running in Docker container due to Docker networking woes (a Docker container by default cannot access network ports of the hosting machine)


Following two solutions that addresses the above with different pros/cons:


### Execute locally

Local execute a command based on the specific event (re-merge, re-commit and etc).
The lakeFS service will execute a command, if found, based on the event.
For each event the service will execute the script and pass all the relevant properties as environment variables.
The location of the scripts will be configurable:

```yaml
hooks_location: ./hooks
```

Commit for example, will execute `./hooks/pre-commit` and `./hooks/post-commit` (if exists).
Running the hook will include settings the event properties as environment variables so the script can implment the relevant logic:

 - LAKEFS_REPOSITORY_ID - repository id
 - LAKEFS_BRANCH_ID - branch name
 - LAKEFS_COMMIT_MESSAGE - commit message
 - LAKEFS_COMMIT_METADATA - commit meta data
 - LAKEFS_COMMITTER - committer name

For merge event will include the LAKEFS_SOURCE_REF as additional property to specify the source commit.

The user should be aware of the security risks of running his script as part of lakeFS environment.
Note that when lakeFS runs inside a container and the user would like to run another container, they will have to install and run docker while connecting to the docker host machine.


### Remote execute using webhooks

Reducing the security risk of running local command and leverage the current lakeFS web hook capabilities. Running another service outside lakeFS can help reduce security risks, interrupt lakeFS execution and will use the same capabilities to execute actions as part of lakeFS (not another mechanism)

The user will need to execute the service / docker container and pass it a token.


```yaml
hooks:
  - id: exec
    type: webhook
    properties:
      url: "http://<exec host:port>/exec"
      timeout: 1m30s
      query_params:
        cmd: /scripts/merge_verifier.sh
      headers:
        x-exec-token: "<exec webhook service token>"
```

The service will populate the incoming request payload as environment variables (see 'Execute locally' environment solution) and execute it.


