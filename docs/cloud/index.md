---
layout: default
title: lakeFS Cloud
description: This section includes lakeFS Cloud documentation
nav_order: 80
has_children: true
redirect_from: "/cloud.html"
has_toc: false
---

# lakeFS Cloud
[lakeFS Cloud](https://lakefs.cloud) is a fully-managed lakeFS solution provided by Treeverse, implemented using our best practices, providing high availability, auto-scaling, support and enterprise-ready features.
	
## lakeFS Cloud Features
* [Role-Based Access Control](../reference/rbac.md)
* [Auditing](./auditing.md)
* [Single-Sign-On](./sso.md) (including support for SAML, OIDC, AD FS, Okta, and Azure AD)
* [Managed Garbage Collection](./managed-gc.md)
* [Private-Link](./private-link.md)
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

### AWS
Please follow the self-service setup wizard on [lakeFS Cloud](https://lakefs.cloud)

### Azure
Please follow [these instructions](./setup-azure.md) to onboard to lakeFS Cloud on Azure.

### GCP
Please [contact us](mailto:support@treeverse.io) for onboarding insructions.