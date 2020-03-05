import base64 from 'base-64';

const API_ENDPOINT = '/api/v1';

const basicAuth = (accessKeyId, secretAccessKey) => {
    return {
        "Authorization": `Basic ${base64.encode(`${accessKeyId}:${secretAccessKey}`)}`,
    };
};

const cachedBasicAuth = () => {
    let userData = window.localStorage.getItem("user");
    userData = JSON.parse(userData);
    return basicAuth(userData.accessKeyId, userData.secretAccessKey);
};

export const json =(data) => {
    return JSON.stringify(data, null, "");
};

export async function extractError(response) {
    let body;
    if (response.headers.get('Content-Type') === 'application/json') {
        body = await response.json().message;
    } else {
        body = await response.text();
    }
    return body;
}

export default async function(uri, requestData = {}, additionalHeaders = {}, credentials = null) {
    const auth = (credentials === null) ?
        cachedBasicAuth() : basicAuth(credentials.accessKeyId, credentials.secretAccessKey);

    return await fetch(`${API_ENDPOINT}${uri}`, {
        headers: new Headers({
            ...auth,
            "Accept": "application/json",
            "Content-Type": "application/json",
            ...additionalHeaders,
        }),
        ...requestData,
    });
};
