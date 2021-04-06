# Before Installing lakeFS

Prior to installing lakeFS, let's go over some basic notions about how lakeFS is built.

## Interacting with lakeFS

In order to perform actions on lakeFS, you can use the following APIs:

1. **The lakeFS OpenAPI**: used by the `lakectl` CLI tool. Exposes git-like operations (branching, diffing, merging etc.).
1. **An S3 compatible API**: read and write your data in any tool that can communicate with S3. Examples include: AWS CLI, Boto, Presto and Spark.  


## Choosing Endpoint DNS

lakeFS actually exposes only one API endpoint. For every request, lakeFS checks the `Host` header, and uses the domain to direct the request to the correct API.

To install lakeFS, you need to choose a domain for each API. 
A good convention for this is, assuming you own the domain `example.com`:

* `lakefs.example.com` for the OpenAPI.
* `s3.lakefs.example.com` for the S3-compatible API.
* `*.s3.lakefs.example.com` for [virtual-host style access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html) to the S3-compatible API.

You should create DNS records for these addresses, all pointing to the lakeFS server.

Later, when configuring lakeFS, you will set the property `gateways.s3.domain_name` to `s3.lakefs.example.com`.

