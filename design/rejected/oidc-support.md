_This design has moved out of scope of lakeFS, see our [security
update](https://docs.lakefs.io/posts/security_update.html)._

# OIDC support in lakeFS

As suggested by @arielshaqed, we started researching the option to implement OIDC support as part of lakeFS.

In such a solution, the person who configures lakeFS will supply the parameters required for implementing OIDC, including the provider's domain, the client ID and secret, and a callback URL. API access will still use credentials managed internally like today.

As a first step, authorization will still be managed internally by lakeFS.

## Requirements


### lakeFS admin

- As part of the lakeFS configuration file, you can enable an integration with an OIDC provider
- Through the lakeFS configuration, the admin can set default lakeFS IAM groups. By default, externally managed users will be created with these groups.
- To create users with _other_ groups - an "initial_groups" claim can be added on the ID token through the external provider. When the user first logs in, lakeFS will read this claim and create the user with the given groups instead of the default ones.
- Authorization is still managed internally by lakeFS. Since we don't save any PII, the permission management for users will show only the unique ID provided by the OIDC provider.
  - To show user emails / friendly names in permission management screens, we will integrate with specific OIDC providers according to demand by the community.

### lakeFS user
- In the lakeFS login screen, the user can choose to login with OIDC.
- The user can create API credentials as before.

