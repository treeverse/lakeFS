## Prerequisites

{: .no_toc }

Users that require S3 access using [virtual host addressing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html) will require three DNS records **pointing at your lakeFS server**.
A good convention for those will be, assuming you already own the domain `example.com`:
* `lakefs.example.com`
* `s3.lakefs.example.com` - **this is the explicit S3 Gateway domain**
* `*.s3.lakefs.example.com`

The second record, the *S3 Gateway Domain*, needs to be specified in the lakeFS configuration.
This will allow lakeFS to route requests to the S3-compatible API using a virtual host as a bucket name.

