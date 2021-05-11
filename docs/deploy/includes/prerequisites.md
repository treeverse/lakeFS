## Prerequisites

{: .no_toc }
A production-suitable lakeFS installation will require three DNS records **pointing at your lakeFS server**.
A good convention for those will be, assuming you already own the domain `example.com`:

* `lakefs.example.com`
* `s3.lakefs.example.com` - **this is the S3 Gateway Domain**
* `*.s3.lakefs.example.com`

The second record, the *S3 Gateway Domain*, is used in lakeFS configuration to differentiate between the S3 Gateway API and the OpenAPI Server. For more info, see [Why do I need these three DNS records?](#why-do-i-need-the-three-dns-records)

Find your preferred installation method:

If you just want to try out lakeFS locally, see [Quickstart](../quickstart/index.md).

## Why do I need the three DNS records?
{: .no_toc }

Multiple DNS records are needed to access the two different lakeFS APIs (covered in more detail in the [Architecture](../architecture/overview.md) section):

1. **The lakeFS OpenAPI**: used by the `lakectl` CLI tool. Exposes git-like operations (branching, diffing, merging etc.).
1. **An S3-compatible API**: read and write your data in any tool that can communicate with S3. Examples include: AWS CLI, Boto, Presto and Spark.

lakeFS actually exposes only one API endpoint. For every request, lakeFS checks the `Host` header.
If the header is under the S3 gateway domain, the request is directed to the S3-compatible API.

The third DNS record (`*.s3.lakefs.example.com`) allows for [virtual-host style access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html). This is a way for AWS clients to specify the bucket name in the Host subdomain.
