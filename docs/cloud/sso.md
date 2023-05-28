---
layout: default
title: Single Sign On (SSO)
description: How to configure Single Sign On (SSO) for lakeFS Cloud.
parent: lakeFS Cloud
has_children: false
has_toc: false
---

# Single Sign On (SSO) for lakeFS Cloud
{: .d-inline-block }
lakeFS Cloud
{: .label .label-green }

{: .note}
> SSO is also available for [lakeFS Enterprise](/enterprise/sso.html). Using the open-source version of lakeFS? Read more on [authentication](/reference/authentication.html). 

lakeFS Cloud uses Auth0 for authentication and thus support the same identity providers as Auth0 including Active Directory/LDAP, ADFS, Azure Active Directory Native, Google Workspace, OpenID Connect, Okta, PingFederate, SAML, and Azure Active Directory.

<div class="tabs">
  <ul>
    <li><a href="#okta">Okta</a></li>
    <li><a href="#adfs">AD FS</a></li>
    <li><a href="#azure-ad">Azure AD</a></li>
  </ul> 
  <div markdown="1" id="okta">
## Okta

{: .note}
> This guide is based on [Okta's Create OIDC app integrations guide](https://help.okta.com/en-us/Content/Topics/Apps/Apps_App_Integration_Wizard_OIDC.htm).

Steps:
1. Login to your Okta account
2. Select **Applications > Applications**, then **Create App Integration**.
3. Select Create New App and enter the following:
    1. For **Sign-in method**, choose OIDC.
    2. Under **Application type**, choose **Web app**.
    3. Select Next.
4. Under General Settings:
    1. **App integration name**, enter a name for your application. (i.e lakeFS Cloud)
5. In the **Sign-in redirect URIs** field, enter https://lakefs-cloud.us.auth0.com/login (United States) or https://lakefs-cloud.eu.auth0.com/login (Europe).
6. Under **Sign-in redirect URIs**, click **Add URI**, enter https://lakefs-cloud.us.auth0.com/login/callback (United States) or https://lakefs-cloud.eu.auth0.com/login/callback (Europe).
7. Under **Assignments**, choose the wanted **Controlled access**. (i.e Allow everyone in your organization to access)
8. Uncheck **Enable immediate access with Federation Broker Mode**.
9. Select **Save**.

Once you finish registering your application with Okta, save the **Client ID**, **Client Secret** and your **Okta Domain**, send this to Treeverse's team to finish the integration.
  </div>
  <div markdown="1" id="adfs">
## Active Directory Federation Services (AD FS)

Prerequisites:
* Client's AD FS server should be exposed publicly or to Auth0's IP ranges (either directly or using Web Application Proxy)

Steps:
1. Connect to the AD FS server
2. Open AD FS' PowerShell CLI as Administrator through the server manager
3. Execute the following:

    ```sh
    (new-object Net.WebClient -property @{Encoding = [Text.Encoding]::UTF8}).DownloadString("https://raw.github.com/auth0/adfs-auth0/master/adfs.ps1") | iex

    AddRelyingParty "urn:auth0:lakefs-cloud" "https://lakefs-cloud.us.auth0.com/login/callback"
    ```

    **Note**: If your organization data is located in Europe, use `lakefs-cloud.eu.auth0.com` instead of `lakefs-cloud.us.auth0.com`.

Once you finish registering lakeFS Cloud with AD FS, save the **AD FS URL** and send this to Treeverse's team to finish the integration.
  </div>
  <div markdown="1" id="azure-ad">
## Azure Active Directory (AD)

Prerequisites:
* Azure account with permissions to manage applications in Azure Active Directory

**Note**: If you've already set up lakeFS Cloud with your Azure account, you can skip the [Register lakeFS Cloud with Azure](#register-lakefs-cloud-with-azure) and [Add client secret](#add-a-secret) and go directly to [Add a redirect URI](#add-a-redirect-uri).

### Register lakeFS Cloud with Azure

Steps:
1. Sign in to the Azure portal.
2. If you have access to multiple tenants, use the Directories + subscriptions filter in the top menu to switch to the tenant in which you want to register the application.
3. Search for and select Azure Active Directory.
4. Under Manage, select App registrations > New registration.
5. Enter a display Name for your application. Users of your application might see the display name when they use the app, for example during sign-in. You can change the display name at any time and multiple app registrations can share the same name. The app registration's automatically generated Application (client) ID, not its display name, uniquely identifies your app within the identity platform.
6. Specify who can use the application, sometimes called its sign-in audience.

   Note: don't enter anything for Redirect URI (optional). You'll configure a redirect URI in the next section.
7. Select Register to complete the initial app registration.

When registration finishes, the Azure portal displays the app registration's Overview pane. You see the Application (client) ID. Also called the client ID, this value uniquely identifies your application in the Microsoft identity platform.

Important: new app registrations are hidden to users by default. When you are ready for users to see the app on their My Apps page you can enable it. To enable the app, in the Azure portal navigate to Azure Active Directory > Enterprise applications and select the app. Then on the Properties page toggle Visible to users? to Yes.

### Add a secret
Sometimes called an application password, a client secret is a string value your app can use in place of a certificate to identity itself.

Client secrets are considered less secure than certificate credentials. Application developers sometimes use client secrets during local app development because of their ease of use. However, you should use certificate credentials for any of your applications that are running in production.

Steps:
1. In the Azure portal, in App registrations, select your application.
2. Select Certificates & secrets > Client secrets > New client secret.
3. Add a description for your client secret.
4. Select an expiration for the secret or specify a custom lifetime.
    1. Client secret lifetime is limited to two years (24 months) or less. You can't specify a custom lifetime longer than 24 months.
    2. Microsoft recommends that you set an expiration value of less than 12 months.
5. Select Add.
6. Record the secret's value for use in your client application code. This secret value is never displayed again after you leave this page.

### Add a redirect URI
A redirect URI is the location where the Microsoft identity platform redirects a user's client and sends security tokens after authentication.

You add and modify redirect URIs for your registered applications by configuring their platform settings.

Enter https://lakefs-cloud.us.auth0.com/login/callback or https://lakefs-cloud.eu.auth0.com/login/callback (depends on your organization data location) as your redirect URI.

Settings for each application type, including redirect URIs, are configured in Platform configurations in the Azure portal. Some platforms, like Web and Single-page applications, require you to manually specify a redirect URI. For other platforms, like mobile and desktop, you can select from redirect URIs generated for you when you configure their other settings.

Steps:
1. In the Azure portal, in App registrations, select your application.
2. Under Manage, select Authentication.
3. Under Platform configurations, select Add a platform.
4. Under Configure platforms, select the web option.
5. Select Configure to complete the platform configuration.

Once you finish registering lakeFS Cloud with Azure AD, send the following values to the Treeverse team:
- **Application (Client) ID**
- **Application Secret Value**
- **Microsoft Azure AD Domain** 
  </div>
</div>






