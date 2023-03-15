import argparse
import sys

import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient
from python_on_whales import docker
from lakefs_client.model.setup import Setup
from lakefs_client.model.access_key_credentials import AccessKeyCredentials


def flatten(lst):
    return [item for sublist in lst for item in sublist]


def get_spark_submit_cmd(submit_flags, spark_config, jar_name, jar_args):
    cmd = ["spark-submit", "--master", "spark://spark:7077"]
    cmd.extend(submit_flags)
    configs = flatten([['-c', f"{k}={v}"] for k, v in spark_config.items()])
    cmd.extend(configs)
    cmd.extend(["--class", "Sonnets", f"/local/app/target/{jar_name}"])
    cmd.extend(jar_args)
    return cmd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage_namespace", default="local://", required=True)
    parser.add_argument("--repository", default="example", required=True)
    parser.add_argument("--sonnet_jar", required=True)
    parser.add_argument("--client_version")
    parser.add_argument("--aws_access_key")
    parser.add_argument("--aws_secret_key")
    parser.add_argument("--region")
    parser.add_argument("--direct_access", action="store_true")
    lakefs_access_key = 'AKIAIOSFODNN7EXAMPLE'
    lakefs_secret_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

    args = parser.parse_args()
    if args.client_version:
        submit_flags = ["--packages", "io.lakefs:hadoop-lakefs-assembly:{args.client_version}"]
    else:
        submit_flags = ["--jars", "/target/client.jar"]

    lfs_client = LakeFSClient(
        lakefs_client.Configuration(username=lakefs_access_key,
                                    password=lakefs_secret_key,
                                    host='http://localhost:8000'))

    lfs_client.config.setup(Setup("tester", key=AccessKeyCredentials(lakefs_access_key, lakefs_secret_key)))
    lfs_client.repositories.create_repository(
        models.RepositoryCreation(name=args.repository,
                                  storage_namespace=args.storage_namespace,
                                  default_branch='main',))

    with open('./app/data-sets/sonnets.txt', 'rb') as f:
        lfs_client.objects.upload_object(repository=args.repository, branch="main", path="sonnets.txt", content=f)

    if args.direct_access:
        scheme = "lakefs"
        spark_configs = {
            "spark.hadoop.fs.lakefs.impl": "io.lakefs.LakeFSFileSystem",
            "spark.driver.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
            "spark.executor.extraJavaOptions": "-Dcom.amazonaws.services.s3.enableV4=true",
            "spark.hadoop.fs.lakefs.endpoint": "http://lakefs:8000/api/v1",
            "spark.hadoop.fs.lakefs.access.key": lakefs_access_key,
            "spark.hadoop.fs.lakefs.secret.key": lakefs_secret_key,
            "spark.hadoop.fs.s3a.access.key": args.aws_access_key,
            "spark.hadoop.fs.s3a.secret.key": args.aws_secret_key,
            "spark.hadoop.fs.s3a.region": args.region,
        }
    else:
        scheme = "s3a"
        spark_configs = {"spark.hadoop.fs.s3a.access.key": lakefs_access_key,
                         "spark.hadoop.fs.s3a.secret.key": lakefs_secret_key,
                         "spark.hadoop.fs.s3a.endpoint": "s3.docker.lakefs.io:8000",
                         "spark.hadoop.fs.s3a.connection.ssl.enabled": "false"}

    generator = docker.compose.run("spark-submit",
                                   get_spark_submit_cmd(submit_flags, spark_configs, args.sonnet_jar,
                                                        [f"{scheme}://{args.repository}/main/sonnets.txt",
                                                         f"{scheme}://{args.repository}/main/sonnets-wordcount"]),
                                   dependencies=False,
                                   tty=False,
                                   stream=True,
                                   name="submit")

    for _, stream_content in generator:
        print(stream_content.decode(), end="")
    state = docker.container.inspect("submit").state
    if state.exit_code != 0:
        print(state.error)
    docker.container.remove("submit")
    sys.exit(state.exit_code)


if __name__ == '__main__':
    main()
