---
layout: default
parent: posts
has_children: false
date: 2023-01-31
search_exclude: true
---

# Security update for the lakeFS project

Trust is our core value, and security topics are never taken lightly. 

We heard you, and after many conversations, we decided to move from decoupled security authentication and access control features to enable you to plug your own authentication and security mechanism. 

We understand that focusing on building lakeFS to be the best data version control tool is the open-source project north star. 
Consequently, we decided to change the architecture to a pluggable one which enables you to choose your preference without being dependent on the lakeFS solution. 


## What’s changing?

With lakeFS [v0.91.0](https://github.com/treeverse/lakeFS/releases/tag/v0.91.0){: target="_blank" } Role-based access control and OIDC authentication are deprecated due to this architecture change. 
Within the lakeFS UI, you will see a deprecation notice on the administration screens when trying to update and create policies.

In coming versions, we plan on making authorization and authentication pluggable, while bundling a simpler reference implementation into the core of lakeFS.
This implementation will include basic identity management with built-in users and groups, as well as a simplified authorization mechanism based on ACLs.

**If you currently rely on any of the deprecated features, please [contact us](mailto:support@treeverse.io?subject=RBAC+and+OIDC+deprecation){: target="_blank" } - we'll do our best to support you in the transition.**
{: .note }

## Why are we making this architectural change?

This change is necessary as it lets you decide on access control and authentication that best fits your security needs. It matters since one can ensure security only if one controls the full environment.

lakeFS users will be able to develop and use existing identity providers and wrap lakeFS with their own authorization logic (if they so desire) - to meet their specific security needs.


## How can I ensure security?

Now that the architecture is pluggable, you can use the solution of your choice. 

Depending on your needs, you might want to take a look at lakeFS Cloud, which enables us to provide a holistic, secure solution that is SOC2 compliant and provides security guarantees.

lakeFS Cloud will continue to support and maintain more advanced authentication methods such as SAML and OIDC, and granular access control in the form of role-based access control policies. Additionally, lakeFS Cloud also provides a full, queryable auditing log to help administrators gain full visibility on data access within their lakeFS environment.

---

Trust is our core value, and security topics are never taken lightly. 

Even though this decision was difficult, it was necessary to continue building a solid foundation for lakeFS's data version control.

We remain [committed](../commitment.html) to making lakeFS the best, most scalable, open source version control system for data practitioners, while not compromising on its security.



_Oz & Einat_