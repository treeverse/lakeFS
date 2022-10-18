---
layout: default
title: "S3 Virtual-host addressing (advanced)"
description: Configure the lakeFS S3 API to use virtual-host addressing.
parent: Set up lakeFS
has_children: false
---

# Configuring lakeFS to use S3 Virtual-Host addressing

## Understanding virtual-host addressing

Some systems require S3 endpoints (such as lakeFS' [S3 Gateway](../understand/architecture.md#s3-gateway)) to support [virtual-host style addressing](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html){: target="_blank" }.

lakeFS supports this, but requires some configuration in order to extract the bucket name (used as the lakeFS repository ID) from the host address.

For example:

```text
GET http://foo.example.com/some/location
```

In this case, there's no way for lakeFS to determine whether this is a virtual-host request where the endpoint url is `example.com`, the bucket name is `foo` and the path is `/some/location`,
or a path-based request where the endpoint is `foo.example.com`, the bucket name is `some` and the path is `location`.

This requires an extra step: defining an explicit set of DNS record for lakeFS S3 gateway.

## Adding an explicit S3 domain name to the S3 Gateway configuration

The first step would be to tell the lakeFS installation which hostnames are used for the S3 Gateway. This should be a different DNS record from the one used for e.g. the UI or API.

Typically, if the lakeFS installation is served under `lakefs.example.com`, a good choice would be `s3.lakefs.example.com`.

This could be done using either an environment variable:

```shell
LAKEFS_GATEWAYS_S3_DOMAIN_NAME="s3.lakefs.example.com"
```

Or by adding the `gateways.s3.domain_name` setting to the lakeFS `config.yaml` file:

```yaml
---
database:
  connection_string: "..."

...

# This section defines an explict S3 gateway address that supports virtual-host addressing
gateways:
  s3:
    domain_name: s3.lakefs.example.com
```

For more information on how to configure lakeFS, check out the [configuration reference](../reference/configuration.md)
{: .note }

## Setting up the appropriate DNS records

Once your lakeFS installation is configured with an explicit S3 gateway endpoint address, you need to define two DNS records and have them point at your lakeFS installation.
This requires two CNAME records:

1. `s3.lakefs.example.com` - CNAME to `lakefs.example.com`. This will be used as the S3 endpoint when configuring clients and serve as our bare domain.
1. `*.s3.lakefs.example.com` - Also a CNAME to `lakefs.example.com`. This will resolve virtual-host requests such as `example-repo.s3.lakefs.example.com` that lakeFS would now know how to parse.


<div class="note">
   <p>For more information on how to configure these, see the official documentation of your DNS provider.</p>
   <p>On AWS, This could also be done [using ALIAS records](https://aws.amazon.com/premiumsupport/knowledge-center/route-53-create-alias-records/) for a load balancer.</p> 
</div>
