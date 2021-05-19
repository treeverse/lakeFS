## Prerequisites

{: .no_toc }
A production-suitable lakeFS installation will require three DNS records **pointing at your lakeFS server**.
A good convention for those will be, assuming you already own the domain `example.com`:
* `lakefs.example.com`
* `s3.lakefs.example.com` - **this is the S3 Gateway Domain**
* `*.s3.lakefs.example.com`

The second record, the *S3 Gateway Domain*, needs to be specified in the lakeFS configuration (see the `S3_GATEWAY_DOMAIN` placeholder below). This will allow lakeFS to route requests to the S3-compatible API. For more info, see [Why do I need these three DNS records?](#why-do-i-need-the-three-dns-records)
