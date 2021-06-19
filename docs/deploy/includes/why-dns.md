# Why do I need the three DNS records?

{: .no\_toc }

Multiple DNS records are needed to access the two different lakeFS APIs \(covered in more detail in the [Architecture](https://github.com/treeverse/lakeFS/tree/9d35f14eba038648902a59dbb091b27590525e87/docs/understand/architecture.html) section\):

1. **The lakeFS OpenAPI**: used by the `lakectl` CLI tool. Exposes git-like operations \(branching, diffing, merging etc.\).
2. **An S3-compatible API**: read and write your data in any tool that can communicate with S3. Examples include: AWS CLI, Boto, Presto and Spark.

lakeFS actually exposes only one API endpoint. For every request, lakeFS checks the `Host` header. If the header is under the S3 gateway domain, the request is directed to the S3-compatible API.

The third DNS record \(`*.s3.lakefs.example.com`\) allows for [virtual-host style access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html). This is a way for AWS clients to specify the bucket name in the Host subdomain.

