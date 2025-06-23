---
title: Migrate from Fluffy to lakeFS Enterprise
description: How to migrate from the legacy Fluffy-based enterprise setup to the new unified lakeFS Enterprise
---

# Migrate From Fluffy to lakeFS Enterprise


The new lakeFS Enterprise integrates all enterprise features directly into a single binary, eliminating the need for the separate Fluffy service. This simplifies deployment, configuration, and maintenance.

## Configuration Changes

### Configuration Mapping

#### License Configuration (New Requirement)
Add license configuration to your lakeFS Enterprise config:

```yaml
license:
  path: "/path/to/license.jwt"
  # OR
  # contents: "eyJhbGciOiJSUzI1NiIs..."
```

#### Authentication configuration
Most Fluffy `auth.*` settings move directly to lakeFS Enterprise with the same structure. Below are added the differences between 
the configurations.

**Fluffy SAML Config (Old):**
```yaml
# fluffy.yaml
auth:
  encrypt:
    secret_key: "your-secret"
  logout_redirect_url: "https://auth0.com/v2/logout"
  saml:
    enabled: true 
    external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name
```

**lakeFS SAML Enterprise Config (New):**
```yaml
# lakefs.yaml
auth:
  encrypt:
    secret_key: "your-secret"
  logout_redirect_url: "https://auth0.com/v2/logout"
  cookie_auth_verification:
    external_user_id_claim_name: http://schemas.xmlsoap.org/ws/2005/05/identity/claims/name # This field was moved here!
  providers:
    saml:
      # enabled: true  // This field was dropped! 
```

**Fluffy OIDC Config (Old):**
```yaml
# fluffy.yaml
auth:
  encrypt:
    secret_key: "your-secret"
  logout_redirect_url: "https://auth0.com/v2/logout"
  post_login_redirect_url: http://localhost:8000/
  oidc:
    enabled: true
```

**lakeFS OIDC Enterprise Config (New):**
```yaml
# lakefs.yaml
auth:
  encrypt:
    secret_key: "your-secret"
  logout_redirect_url: "https://auth0.com/v2/logout"
  oidc:
    post_login_redirect_url: http://localhost:8000/ # This field was moved here!
    enabled: true
```
#### Database Configuration
Use the same database configuration, but only in the lakeFS Enterprise config:

```yaml
database:
  type: postgres
  postgres:
    connection_string: "postgres://user:pass@host:5432/db?sslmode=disable"
```

#### Removed Configurations
These Fluffy-specific settings are no longer needed:

- `auth.serve_listen_address` - Internal communication
- `auth.serve_disable_authentication` - Internal communication  
- `listen_address` - Handled by lakeFS Enterprise
- Fluffy's `logging` settings (United with lakeFS logging settings)
- Fluffy's `database` settings (United with lakeFS database settings)