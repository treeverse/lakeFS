## Prerequisites

{: .no_toc }
A production-suitable lakeFS installation will require three DNS records **pointing at your lakeFS server**.
A good convention for those will be, assuming you already own the domain `example.com`:
* `lakefs.example.com`
* `s3.lakefs.example.com` - **this is the S3 Gateway Domain**
* `*.s3.lakefs.example.com`

The second record, the *S3 Gateway Domain*, is used in lakeFS configuration to differentiate between the S3 Gateway API and the OpenAPI Server. For more info, see [Why do I need these three DNS records?](#why-do-i-need-the-three-dns-records)
