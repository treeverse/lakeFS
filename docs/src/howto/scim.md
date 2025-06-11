---
title: System for Cross-domain Identity Management (SCIM)
description: Use SCIM to automatically provision users/groups in lakeFS via your identity provider (IdP)
status: enterprise
---

# System for Cross-domain Identity Management (SCIM)

!!! info
    SCIM support is available on **lakeFS Cloud** and **lakeFS Enterprise**

lakeFS Cloud includes an [SCIM v2.0](https://datatracker.ietf.org/doc/html/rfc7644) compliant server, which can integrate with SCIM clients (IdPs) to automate provisioning/de-provisioning of users and groups.  


## Officially Supported Clients (IdPs), Capabilities, and Limitations

### Supported Clients (IdPs)

Currently, the lakeFS Cloud SCIM server has been tested and validated with [Entra ID](https://www.microsoft.com/en-us/security/business/identity-access/microsoft-entra-id) (a.k.a Azure AD). However, with SCIM v2.0 being an accepted standard, any SCIM-compliant IdP should be able to integrate with lakeFS Cloud.

### Capabilities

### User Provisioning

- **Create users:** Users and members of groups assigned to the application will be provisioned in lakeFS Cloud
- **Update user attributes:** Changes to supported user attributes are synced to lakeFS Cloud
- **Deactivate users:** Deactivating a user or removing their assignment to the application will disable them in lakeFS Cloud
- **User adoption:** Users that are already found in lakeFS Cloud will be "adopted" by the IdP and not re-created

### Group Provisioning

- **Create groups:** Groups assigned to the application are created in lakeFS Cloud and any group user members are created and added to the group in lakeFS Cloud
- **Update group name:** When a synced group is renamed in the IdP, it will be renamed in lakeFS Cloud
- **Add/remove members:** When members are added/removed from an assigned group, they will be added/removed from the group in lakeFS Cloud
- **Group adoption:** Groups that already exist in lakeFS Cloud will be "adopted" by the IdP and not re-created

### User Attributes and Consent

The lakeFS Cloud SCIM server requires the minimum set of user attributes required for provisioning. The required attributes are a sub-set of the basic user profile, which is exchanged during federated authentication/SSO login. User consent is requested by the IdP upon first login to lakeFS Cloud.

### Known Limitations

- User and group policies can only be managed in lakeFS  
  This means groups and users newly created via SCIM only have basic read permissions. The lakeFS UI or API must be used to attach policies to those users and groups. However, if a user is created and added to an existing group with an attached policy, that user will receive the permissions allowed by the policy attached to the group.
- Only direct group memberships are provisioned via SCIM  
  Both Okta and Entra ID only support syncing direct group membership via SCIM. This means that if you assign a group to the application, only its user members will be provisioned via SCIM. SCIM provisioning will not cascade to member groups and their members, and so forth.

## Enabling SCIM in lakeFS Cloud

To enable SCIM support in lakeFS Cloud, you need to log into the cloud admin. In the cloud admin, SCIM settings are under **Access > Settings**. SCIM is not enabled by default, so to enable SCIM for the organization, click the **Setup Provisioning** Button.

![lakeFS Cloud SCIM Settings](../assets/img/scim/lakefs_cloud_scim_settings.png)

Clicking the button will enable SCIM for the organization and provide the details you'll need to set up your IdP to work with lakeFS Cloud SCIM.

![lakeFS Cloud SCIM Configuration](../assets/img/scim/lakefs_cloud_scim_configuration.png)

To set up your IdP, you'll need the lakeFS Cloud SCIM provisioning endpoint and you'll also need to generate an integration token. When creating a new integration token, you can optionally provide a description for future reference.

!!! info
    The token value is only presented once, right after creation. Make sure to copy the token, as its value isn't stored and cannot be retrieved after the initial creation.

## Setting Up SCIM Provisioning in Entra ID (a.k.a Azure AD)

!!! note
    This guide assumes you've already set up an Entra ID enterprise application for federated authentication to lakeFS Cloud.

In the Entra ID admin dashboard, go to **Enterprise Applications** and choose the lakeFS Cloud enterprise application from the list. Then click **Provisioning** in the sidebar and then **Get Started**.

1. In the provisioning settings set mode to **Automatic**
2. In **Tenant URL** enter the URL from the lakeFS Cloud provisioning settings. You will need to append `?aadOptscim062020` to the end of the URL to ensure proper integration with Entra ID.
3. In **Secret Token** paste the token you copied in the previous step. If you haven't created a token yet, you may do so now
4. Click **Test Connection**
5. If the test fails, please ensure you've entered the correct SCIM endpoint URL from lakeFS Cloud and copied the token correctly. Otherwise, click "Save" at the top of the settings panel
6. Configure provisioning attribute mappings(this determines which attributes are sent to the lakeFS SCIM endpoint)

## Required Attributes
The LakeFS SCIM implementation has a number of attributes that it expects to see in requests. Missing, incorrect, or extraneous attributes will generally result in a 400 error code.

### User Resource Attributes
  * username: Unique identifier for the User, typically used by the user to directly authenticate to the service provider. Each User MUST include a non-empty userName value. This identifier MUST be unique across the service provider's entire set of Users. REQUIRED.
  * externalId: A String that is an identifier for the resource as defined by the provisioning client. REQUIRED.
  * emails: Email addresses for the user. The value SHOULD be canonicalized by the service provider, e.g., 'bjensen@example.com' instead of 'bjensen@EXAMPLE.COM'. Canonical type values of 'work', 'home', and 'other'. One should be marked as primary. REQUIRED.
  * active: A Boolean value indicating the User's administrative status.

### Group Resource Attributes
  * displayName: A human-readable name for the Group. REQUIRED.
  * externalId: A String that is an identifier for the resource as defined by the provisioning client. REQUIRED.
  * members: A list of members of the Group.


!!! info
    lakeFS Cloud is designed to work with the default attribute mapping for users and groups provided by Entra ID.
    If your organization has customized the user and/or group entities in Entra ID, you might want to set mappings in accordance with those.
    You can find details of how this is done in the [Entra ID documentation](https://learn.microsoft.com/en-us/entra/identity/app-provisioning/customize-application-attributes).  
    
    Incorrectly modifying these mappings can break provisioning functionality, so it's advised to do so cautiously and only when necessary.
