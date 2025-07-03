import { MockServer } from './mockServer';

const BASE_API_URL = '/api/v1';

export interface UserInfo {
  id: string;
  creation_date: number;
  friendly_name?: string;
  email?: string;
}

export function mockAuthenticated(server: MockServer, userInfo?: Partial<UserInfo>) {
  const defaultUser: UserInfo = {
    id: "test@example.com",
    creation_date: Math.floor(Date.now() / 1000)
  };

  server.setApiMock(`${BASE_API_URL}/user`, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      user: { ...defaultUser, ...userInfo }
    })
  });
}

export function mockUnauthenticated(server: MockServer) {
  server.setApiMock(`${BASE_API_URL}/user`, {
    status: 401,
    contentType: 'application/json',
    body: JSON.stringify({
      message: "Unauthorized"
    })
  });
}

export function mockLicense(server: MockServer, token: string) {
  server.setApiMock(`${BASE_API_URL}/license`, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({ token })
  });
}

export function mockLicenseUnauthorized(server: MockServer) {
  server.setApiMock(`${BASE_API_URL}/license`, {
    status: 401,
    contentType: 'application/json',
    body: JSON.stringify({
      message: "Unauthorized"
    })
  });
}

export function mockLicenseNotImplemented(server: MockServer) {
  server.setApiMock(`${BASE_API_URL}/license`, {
    status: 501,
    contentType: 'application/json',
    body: JSON.stringify({
      message: "Not Implemented"
    })
  });
}

export function mockLicenseServerError(server: MockServer) {
  server.setApiMock(`${BASE_API_URL}/license`, {
    status: 500,
    contentType: 'application/json',
    body: JSON.stringify({
      message: 'Internal Server Error'
    })
  });
}

export function mockLogin(server: MockServer) {
  const now = Math.floor(Date.now() / 1000); 
  const expirationTime = now + 3600;
  
  server.setApiMock(`${BASE_API_URL}/auth/login`, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      token: "mock-auth-token",
      token_expiration: expirationTime
    })
  });
}

export function mockLogout(server: MockServer, logoutRedirectURL: string = '/auth/login') {
  server.setApiMock('/logout', {
    status: 307, 
    headers: {
      'Location': logoutRedirectURL
    }
  });
}

export function mockConfig(server: MockServer) {
  server.setApiMock(`${BASE_API_URL}/config`, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      storage_config: {
        blockstore_description: "",
        blockstore_id: "",
        blockstore_namespace_ValidityRegex: "^local://",
        blockstore_namespace_example: "local://example-bucket/",
        blockstore_type: "local",
        default_namespace_prefix: "local://",
        import_support: false,
        import_validity_regex: "^local://",
        pre_sign_multipart_upload: false,
        pre_sign_support: false,
        pre_sign_support_ui: false
      },
      storage_config_list: [],
      version_config: {
        latest_version: "1.60.0",
        upgrade_recommended: false,
        version: "1.60.0",
        version_context: "lakeFS-Enterprise"
      }
    })
  });
}

export function mockSetupLakefs(server: MockServer) {
  server.setApiMock(`${BASE_API_URL}/setup_lakefs`, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      comm_prefs_missing: false,
      login_config: {
        RBAC: "internal",
        login_cookie_names: [
          "internal_auth_session"
        ],
        login_failed_message: "The credentials don't match.",
        login_url: "",
        logout_url: ""
      },
      state: "initialized"
    })
  });
}

export function mockRepositories(server: MockServer) {
  server.setApiMock(`${BASE_API_URL}/repositories`, {
    status: 200,
    contentType: 'application/json',
    body: JSON.stringify({
      pagination: {
        has_more: false,
        max_per_page: 1000,
        next_offset: "",
        results: 0
      },
      results: []
    })
  });
}