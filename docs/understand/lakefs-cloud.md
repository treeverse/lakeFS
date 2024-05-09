---
title: lakeFS Cloud
description: lakeFS Cloud is a fully-managed lakeFS solution
parent: Understanding lakeFS
redirect_from: 
    - /cloud.html
    - /cloud/index.html
---

# lakeFS Cloud

[lakeFS Cloud](https://lakefs.cloud) is a fully-managed lakeFS solution provided by Treeverse, implemented using our best practices, providing high availability, auto-scaling, support and enterprise-ready features.
	
## lakeFS Cloud Features
* [Role-Based Access Control]({% link reference/security/rbac.md %})
* [Auditing]({% link reference/auditing.md %})
* [Single-Sign-On]({% link reference/security/sso.md %}#sso-for-lakefs-cloud) (including support for SAML, OIDC, AD FS, Okta, and Azure AD)
* [Managed Garbage Collection]({% link howto/garbage-collection/managed-gc.md %})
* [Private-Link]({% link howto/private-link.md %})
* [Unity Delta Sharing]({% link howto/unity-delta-sharing.md %})
* SOC 2 Type II Compliance

## How lakeFS Cloud interacts with your infrastructure

Treeverse hosts and manages a dedicated lakeFS instance that interfaces with data held in your object store, such as S3. 

```mermaid
flowchart TD
    U[Users] --> LFC

    subgraph Your Infrastructure
    IAMM[lakeFS Managed GC IAM Role] --> ObjectStore[Client's Object Store]
    IAMA[lakeFS Application IAM Role] --> ObjectStore
    end

    subgraph Treeverse's Infrastructure
    MGC[Managed Garbage Collection] --> EMR[Elastic Map Reduce]
    EMR --> IAMM
    MGC --> CP
    CP[Control Plane]
    LFC --> CP

        subgraph Client's Tenant
        LFC[lakeFS Cloud] --> DB[Refstore Database]
        end
        
    LFC --> IAMC[lakeFS Connector IAM Role]    
    IAMC -->|ExternalID| IAMA
    end
```

## Setting up lakeFS Cloud

### AWS / Azure
Please follow the self-service setup wizard on [lakeFS Cloud](https://lakefs.cloud)

### GCP
Please [contact us](mailto:support@treeverse.io) for onboarding instructions.

## Scalability Model

By default, a lakeFS Cloud installation supports:
- 1,500 read operations/second across all branches on all repositories within a region
- 1,500 write operations per second across all branches on all repositories within a region

This limit can be increased by contacting [support]((mailto:support@treeverse.io)). 

Each lakeFS branch can sustain up to a maximum of 1,000 write operations/second and 3,000 read operations per second. 
This scales horizontally, so for example, with 10 concurrent branches, a repository could sustain 10k writes/second and 30k reads/second, assuming load is distributed evenly between them.

Reading committed data (e.g. from a commit ID or tag) could be scaled up horizontally to any desired capacity, and defaults to ~5,000 reads/second.

