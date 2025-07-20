import { MockServer } from './mockServer';

const BASE_API_URL = '/api/v1';

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

export function mockLogout(server: MockServer, logoutRedirectURL: string = '/auth/login') {
  server.setApiMock('/logout', {
    status: 307,
    headers: {
      'Location': logoutRedirectURL
    }
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