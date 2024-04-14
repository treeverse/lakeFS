---
title: Login to lakeFS with AWS IAM Roles
description: This section covers how to authenticate to lakeFS using AWS IAM.
grand_parent: Reference
parent: Security
redirect_from:
  - /reference/external-principals-aws.html
---

# Authenticate to lakeFS with External Principals API

{: .d-inline-block }
<a style="color: white;" href="#sso-for-lakefs-enterprise">lakeFS Enterprise</a>
{: .label .label-purple }

{: .note}
> External principals API is available for lakeFS Enterprise. If you're using the open-source version you can check the [pluggable APIs](https://docs.lakefs.io/reference/security/rbac.html#pluggable-authentication-and-authorization).

## Overview 

lakeFS supports authenticating users programmatically using AWS IAM roles instead of using static lakeFS access and secret keys.
The method required you to specify bound IAM principal ARNs.
Clients authenticating to lakeFS must have an ARN that matches one of the ARNs bound to the lakefs user they are attempting to login to.
The bound ARN can be attached in 2 modes with SessionName or without it, other then that the ARN must match exactly.
For example, if the bound ARN were `arn:aws:sts::123456:assumed-role/Developer` it would allow any principal assuming `Developer` role in AWS account `123456` to login to it. 
Similarly, if it were `arn:aws:sts::123456:assumed-role/Developer/Foo` it would require the same as above but `Foo` session name. 

## How AWS authentication works

The AWS STS API includes a method, `sts:GetCallerIdentity`, which allows you to validate the identity of a client. The client signs a GetCallerIdentity query using the AWS Signature v4 algorithm and sends it to the lakeFS server. 

The `GetCallerIdentity` query consists of four pieces of information: the request URL, the request body, the request headers, and the request method, as the AWS signature is computed over those fields. The lakeFS server reconstructs the query using this information and forwards it on to the AWS STS service. Depending on the response from the STS service, the server authenticates the client.

Notably, clients don't need network-level access themselves to talk to the AWS STS API endpoint; they merely need access to the credentials to sign the request. However, it means that the lakeFS server does need network-level access to send requests to the STS endpoint.

Each signed AWS request includes the current timestamp to mitigate the risk of replay attacks. In addition, lakeFS allows you to require an additional header, `X-LakeFS-Server-ID` (added by default), to be present to mitigate against different types of replay attacks (such as a signed `GetCallerIdentity` request stolen from a dev lakeFS instance and used to authenticate to a prod lakeFS instance). 

It's also important to note that Amazon does NOT appear to include any sort of authorization around calls to GetCallerIdentity. For example, if you have an IAM policy on your credential that requires all access to be MFA authenticated, non-MFA authenticated credentials will still be able to authenticate to lakeFS using this method.

## Server Configuration



## Authenticate to lakeFS with AWS IAM Roles



## Using with Spark
