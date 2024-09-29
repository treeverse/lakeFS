export const API_ENDPOINT = '/api/v1';
export const DEFAULT_LISTING_AMOUNT = 100;
export const MAX_LISTING_AMOUNT = 1000;

export const SETUP_STATE_INITIALIZED = "initialized";
export const SETUP_STATE_NOT_INITIALIZED = "not_initialized";

class LocalCache {
    get(key) {
        const value = localStorage.getItem(key);
        if (value !== null) {
            return JSON.parse(value);
        }
        return null;
    }

    set(key, value) {
        localStorage.setItem(key, JSON.stringify(value));
    }

    delete(key) {
        localStorage.removeItem(key);
    }
}

const cache = new LocalCache();

export const qs = (queryParts) => {
    const parts = Object.keys(queryParts).map(key => [key, queryParts[key]]);
    return new URLSearchParams(parts).toString();
};

export const linkToPath = (repoId, branchId, path, presign = false) => {
    const query = qs({
        path,
        presign,
    });
    return `${API_ENDPOINT}/repositories/${repoId}/refs/${branchId}/objects?${query}`;
};

export const extractError = async (response) => {
    let body;
    if (response.headers.get('Content-Type') === 'application/json') {
        const jsonBody = await response.json();
        body = jsonBody.message;
    } else {
        body = await response.text();
    }
    return body;
};

export const defaultAPIHeaders = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "X-Lakefs-Client": "lakefs-webui/__buildVersion",
};

const authenticationError = "error authenticating request"

const apiRequest = async (uri, requestData = {}, additionalHeaders = {}) => {
    const headers = new Headers({
        ...defaultAPIHeaders,
        ...additionalHeaders,
    });
    const response = await fetch(`${API_ENDPOINT}${uri}`, {headers, ...requestData});

    // check if we're missing credentials
    if (response.status === 401) {
        const errorMessage = await extractError(response);
        if (errorMessage === authenticationError) {
            cache.delete('user');
            throw new AuthenticationError('Authentication Error', response.status);
        }
        throw new AuthorizationError(errorMessage || 'Unauthorized', response.status);
    }

    return response;
};

// helper errors
export class NotFoundError extends Error {
    constructor(message) {
        super(message)
        this.name = this.constructor.name;
    }
}

export class BadRequestError extends Error {
    constructor(message) {
        super(message)
        this.name = this.constructor.name;
    }
}

export class AuthorizationError extends Error {
    constructor(message) {
        super(message);
        this.name = this.constructor.name;
    }
}

export class AuthenticationError extends Error {
    constructor(message, status) {
        super(message);
        this.status = status;
        this.name = this.constructor.name;
    }
}

export class MergeError extends Error {
    constructor(message, payload) {
        super(message);
        this.name = this.constructor.name;
        this.payload = payload;
    }
}

export class RepositoryDeletionError extends Error {
    constructor(message, repoId) {
        super(message);
        this.name = this.constructor.name;
        this.repoId = repoId;
    }
}

// actual actions:
class Auth {
    async getAuthCapabilities() {
        const response = await apiRequest('/auth/capabilities', {
            method: 'GET',
        });
        switch (response.status) {
            case 200:
                return await response.json();
            default:
                throw new Error('Unknown');
        }
    }

    async login(accessKeyId, secretAccessKey) {
        const response = await fetch(`${API_ENDPOINT}/auth/login`, {
            headers: new Headers(defaultAPIHeaders),
            method: 'POST',
            body: JSON.stringify({access_key_id: accessKeyId, secret_access_key: secretAccessKey})
        });

        if (response.status === 401) {
            throw new AuthenticationError('invalid credentials', response.status);
        }
        if (response.status !== 200) {
            throw new AuthenticationError('Unknown authentication error', response.status);
        }

        this.clearCurrentUser();
        const user = await this.getCurrentUser();

        cache.set('user', user);
        return user;
    }

    clearCurrentUser() {
        cache.delete('user');
    }

    async getCurrentUserWithCache() {
        let user = cache.get('user')
        if (!user) {
            user = await this.getCurrentUser();
            cache.set('user', user);
        }
        return user
    }

    async getCurrentUser() {
        const userResponse = await apiRequest('/user')
        const body = await userResponse.json();
        return body.user;
    }

    async listUsers(prefix = "", after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({prefix, after, amount});
        const response = await apiRequest(`/auth/users?${query}`);
        if (response.status !== 200) {
            throw new Error(`could not list users: ${await extractError(response)}`);
        }
        return response.json();
    }

    async createUser(userId, inviteUser = false) {
        const response = await apiRequest(`/auth/users`,
            {method: 'POST', body: JSON.stringify({id: userId, invite_user: inviteUser})});
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async listGroups(prefix = "", after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({prefix, after, amount});
        const response = await apiRequest(`/auth/groups?${query}`);
        if (response.status !== 200) {
            throw new Error(`could not list groups: ${await extractError(response)}`);
        }
        return response.json();
    }

    async listGroupMembers(groupId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/groups/${encodeURIComponent(groupId)}/members?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list group members: ${await extractError(response)}`);
        }
        return response.json();
    }

    async getACL(groupId) {
        const response = await apiRequest(`/auth/groups/${groupId}/acl`);
        if (response.status !== 200) {
            throw new Error(`could not get ACL for group ${groupId}: ${await extractError(response)}`);
        }
        const ret = await response.json();
        if (ret.repositories === null || ret.repositories === undefined) {
            ret.repositories = [];
        }
        return ret;
    }

    async putACL(groupId, acl) {
        const response = await apiRequest(`/auth/groups/${groupId}/acl`, {
            method: 'POST',
            body: JSON.stringify(acl),
        });
        if (response.status !== 201) {
            throw new Error(`could not set ACL for group ${groupId}: ${await extractError(response)}`);
        }
    }

    async addUserToGroup(userId, groupId) {
        const response = await apiRequest(`/auth/groups/${encodeURIComponent(groupId)}/members/${encodeURIComponent(userId)}`, {method: 'PUT'});
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
    }

    async removeUserFromGroup(userId, groupId) {
        const response = await apiRequest(`/auth/groups/${encodeURIComponent(groupId)}/members/${encodeURIComponent(userId)}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async attachPolicyToUser(userId, policyId) {
        const response = await apiRequest(`/auth/users/${encodeURIComponent(userId)}/policies/${encodeURIComponent(policyId)}`, {method: 'PUT'});
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
    }

    async detachPolicyFromUser(userId, policyId) {
        const response = await apiRequest(`/auth/users/${encodeURIComponent(userId)}/policies/${encodeURIComponent(policyId)}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async attachPolicyToGroup(groupId, policyId) {
        const response = await apiRequest(`/auth/groups/${encodeURIComponent(groupId)}/policies/${encodeURIComponent(policyId)}`, {method: 'PUT'});
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
    }

    async detachPolicyFromGroup(groupId, policyId) {
        const response = await apiRequest(`/auth/groups/${encodeURIComponent(groupId)}/policies/${encodeURIComponent(policyId)}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async deleteCredentials(userId, accessKeyId) {
        const response = await apiRequest(`/auth/users/${encodeURIComponent(userId)}/credentials/${encodeURIComponent(accessKeyId)}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async createGroup(groupName) {
        const response = await apiRequest(`/auth/groups`, {method: 'POST', body: JSON.stringify({id: groupName})});
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async listPolicies(prefix = "", after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({prefix, after, amount});
        const response = await apiRequest(`/auth/policies?${query}`);
        if (response.status !== 200) {
            throw new Error(`could not list policies: ${await extractError(response)}`);
        }
        return response.json();
    }

    async createPolicy(policyId, policyDocument) {
        // keep id after policy document to override the id the user entered
        const policy = {...JSON.parse(policyDocument), id: policyId};
        const response = await apiRequest(`/auth/policies`, {
            method: 'POST',
            body: JSON.stringify(policy)
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async editPolicy(policyId, policyDocument) {
        const policy = {...JSON.parse(policyDocument), id: policyId};
        const response = await apiRequest(`/auth/policies/${encodeURIComponent(policyId)}`, {
            method: 'PUT',
            body: JSON.stringify(policy)
        });
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async listCredentials(userId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/users/${encodeURIComponent(userId)}/credentials?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list credentials: ${await extractError(response)}`);
        }
        return response.json();
    }

    async createCredentials(userId) {
        const response = await apiRequest(`/auth/users/${encodeURIComponent(userId)}/credentials`, {
            method: 'POST',
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async listUserGroups(userId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/users/${encodeURIComponent(userId)}/groups?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list user groups: ${await extractError(response)}`);
        }
        return response.json();
    }

    async listUserPolicies(userId, effective = false, after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const params = {after, amount};
        if (effective) {
            params.effective = 'true'
        }
        const query = qs(params);
        const response = await apiRequest(`/auth/users/${encodeURIComponent(userId)}/policies?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list policies: ${await extractError(response)}`);
        }
        return response.json()
    }

    async getPolicy(policyId) {
        const response = await apiRequest(`/auth/policies/${encodeURIComponent(policyId)}`);
        if (response.status !== 200) {
            throw new Error(`could not get policy: ${await extractError(response)}`);
        }
        return response.json();
    }

    async listGroupPolicies(groupId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/groups/${encodeURIComponent(groupId)}/policies?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list policies: ${await extractError(response)}`);
        }
        return response.json();
    }

    async deleteUser(userId) {
        const response = await apiRequest(`/auth/users/${encodeURIComponent(userId)}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async deleteUsers(userIds) {
        for (let i = 0; i < userIds.length; i++) {
            const userId = userIds[i];
            await this.deleteUser(userId);
        }

    }

    async deleteGroup(groupId) {
        const response = await apiRequest(`/auth/groups/${encodeURIComponent(groupId)}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async deleteGroups(groupIds) {
        for (let i = 0; i < groupIds.length; i++) {
            const groupId = groupIds[i]
            await this.deleteGroup(groupId);
        }
    }

    async deletePolicy(policyId) {
        const response = await apiRequest(`/auth/policies/${encodeURIComponent(policyId)}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async deletePolicies(policyIds) {
        for (let i = 0; i < policyIds.length; i++) {
            const policyId = policyIds[i];
            await this.deletePolicy(policyId);
        }
    }
}

class Repositories {

    async get(repoId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}`);
        if (response.status === 404) {
            throw new NotFoundError(`could not find repository ${repoId}`);
        } else if (response.status === 410) {
            throw new RepositoryDeletionError(`Repository in deletion`, repoId);
        } else if (response.status !== 200) {
            throw new Error(`could not get repository: ${await extractError(response)}`);
        }
        return response.json();
    }

    async list(prefix = "", after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({prefix, after, amount});
        const response = await apiRequest(`/repositories?${query}`);
        if (response.status !== 200) {
            throw new Error(`could not list repositories: ${await extractError(response)}`);
        }
        return await response.json();
    }

    async create(repo) {
        const response = await apiRequest('/repositories', {
            method: 'POST',
            body: JSON.stringify(repo),
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async delete(repoId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }
}

class Branches {

    async get(repoId, branchId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}`);
        if (response.status === 400) {
            throw new BadRequestError('invalid get branch request');
        } else if (response.status === 404) {
            throw new NotFoundError(`could not find branch ${branchId}`);
        } else if (response.status !== 200) {
            throw new Error(`could not get branch: ${await extractError(response)}`);
        }
        return response.json();
    }

    async create(repoId, name, source) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches`, {
            method: 'POST',
            body: JSON.stringify({name, source}),
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response;
    }

    async delete(repoId, name) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(name)}`, {
            method: 'DELETE',
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async reset(repoId, branch, options) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branch)}`, {
            method: 'PUT',
            body: JSON.stringify(options),
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async list(repoId, prefix = "", after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({prefix, after, amount});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list branches: ${await extractError(response)}`);
        }
        return response.json();
    }
}


class Tags {
    async get(repoId, tagId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/tags/${encodeURIComponent(tagId)}`);
        if (response.status === 404) {
            throw new NotFoundError(`could not find tag ${tagId}`);
        } else if (response.status !== 200) {
            throw new Error(`could not get tagId: ${await extractError(response)}`);
        }
        return response.json();
    }

    async list(repoId, prefix = "", after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({prefix, after, amount});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/tags?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list tags: ${await extractError(response)}`);
        }
        return response.json();
    }

    async create(repoId, id, ref) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/tags`, {
            method: 'POST',
            body: JSON.stringify({id, ref}),
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async delete(repoId, name) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/tags/${encodeURIComponent(name)}`, {
            method: 'DELETE',
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

}

class Pulls {
    async get(repoId, pullId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/pulls/${encodeURIComponent(pullId)}`);
        if (response.status === 404) {
            throw new NotFoundError(`Could not find pull request (id = ${pullId}).`);
        } else if (response.status !== 200) {
            throw new Error(`Could not get pull request (id = ${pullId}): ${await extractError(response)}`);
        }
        return response.json();
    }

    async list(repoId, status = "open", prefix = "", after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({status, prefix, after, amount});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/pulls?` + query);
        if (response.status !== 200) {
            const baseMessage = 'Could not list pull requests';
            switch (response.status) {
                case 400:
                case 401:
                case 404:
                    throw new Error(`${baseMessage}: ${(await response.json()).message}`);
                default:
                    throw new Error(`${baseMessage} (status = ${response.status}).`);
            }
        }
        return response.json();
    }

    async create(repoId, pullDetails) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/pulls`, {
            method: 'POST',
            body: JSON.stringify(pullDetails),
        });
        if (response.status !== 201) {
            const baseMessage = 'Could not create pull request';
            switch (response.status) {
                case 400:
                case 401:
                case 403:
                case 404:
                case 409:
                    throw new Error(`${baseMessage}: ${(await response.json()).message}`);
                default:
                    throw new Error(`${baseMessage} (status = ${response.status}).`);
            }
        }
        return response.json()
    }

    async merge(repoId, pullId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/pulls/${encodeURIComponent(pullId)}/merge`, {
            method: 'PUT',
        });
        if (response.status !== 200) {
            const baseMessage = 'Could not merge pull request';
            switch (response.status) {
                case 400:
                case 401:
                case 403:
                case 404:
                case 409:
                case 412:
                    throw new Error(`${baseMessage}: ${(await response.json()).message}`);
                default:
                    throw new Error(`${baseMessage} (status = ${response.status}).`);
            }
        }
    }

    async update(repoId, pullId, pullDetails) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/pulls/${encodeURIComponent(pullId)}`, {
            method: 'PATCH',
            body: JSON.stringify(pullDetails),
        });
        if (response.status !== 204) {
            const baseMessage = 'Could not update pull request';
            switch (response.status) {
                case 400:
                case 401:
                case 403:
                case 404:
                    throw new Error(`${baseMessage}: ${(await response.json()).message}`);
                default:
                    throw new Error(`${baseMessage} (status = ${response.status}).`);
            }
        }
    }
}

// uploadWithProgress uses good ol' XMLHttpRequest because progress indication in fetch() is
//  still not well supported across browsers (see https://stackoverflow.com/questions/35711724/upload-progress-indicators-for-fetch).
export const uploadWithProgress = (url, file, method = 'POST', onProgress = null, additionalHeaders = null) => {
    return new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.upload.addEventListener('progress', event => {
            if (onProgress) {
                onProgress((event.loaded / event.total) * 100);
            }
        });
        xhr.addEventListener('load', () => {
            resolve({
                status: xhr.status,
                body: xhr.responseText,
                contentType: xhr.getResponseHeader('Content-Type'),
                etag: xhr.getResponseHeader('ETag'),
                contentMD5: xhr.getResponseHeader('Content-MD5'),
            })
        });
        xhr.addEventListener('error', () => reject(new Error('Upload Failed')));
        xhr.addEventListener('abort', () => reject(new Error('Upload Aborted')));
        xhr.open(method, url, true);
        xhr.setRequestHeader('Accept', 'application/json');
        xhr.setRequestHeader('X-Lakefs-Client', 'lakefs-webui/__buildVersion');
        if (additionalHeaders) {
            Object.keys(additionalHeaders).map(key => xhr.setRequestHeader(key, additionalHeaders[key]))
        }
        if (url.startsWith(API_ENDPOINT)) {
            // swagger API requires a form with a "content" field
            const data = new FormData();
            data.append('content', file);
            xhr.send(data);
        } else {
            xhr.send(file);
        }
    });
};

class Objects {

    async list(repoId, ref, tree, after = "", presign = false, amount = DEFAULT_LISTING_AMOUNT, delimiter = "/") {
        const query = qs({prefix: tree, amount, after, delimiter, presign});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(ref)}/objects/ls?` + query);

        if (response.status === 404) {
            throw new NotFoundError(response.message ?? "ref not found");
        }

        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return await response.json();
    }

    listAll(repoId, ref, prefix, presign = false) {
        let after = "";
        return {
            next: async () => {
                const query = qs({prefix, presign, after, amount: MAX_LISTING_AMOUNT});
                const response = await apiRequest(
                    `/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(ref)}/objects/ls?` + query);
                if (response.status === 404) {
                    throw new NotFoundError(response.message ?? "ref not found");
                }
                if (response.status !== 200) {
                    throw new Error(await extractError(response));
                }
                const responseBody = await response.json();
                const done = !responseBody.pagination.has_more;
                if (!done) after = responseBody.pagination.next_offset;
                return {page: responseBody.results, done}
            },
        }
    }

    async uploadPreflight(repoId, branchId, path) {
        const query = qs({path});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/objects/stage_allowed?` + query);

        if (response.status === 204) {
            return true;
        }
        if (response.status === 401) {
            return false;
        }

        // This is not one of the expected responses
        throw new Error(await extractError(response));
    }

    async upload(repoId, branchId, path, fileObject, onProgressFn = null) {
        const query = qs({path});
        const uploadUrl = `${API_ENDPOINT}/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/objects?` + query;
        const {status, body, contentType} = await uploadWithProgress(uploadUrl, fileObject, 'POST', onProgressFn)
        if (status !== 201) {
            if (contentType === "application/json" && body) {
                const responseData = JSON.parse(body)
                throw new Error(responseData.message)
            }
            throw new Error(body);
        }
    }

    async delete(repoId, branchId, path) {
        const query = qs({path});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/objects?` + query, {
            method: 'DELETE',
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async get(repoId, ref, path, presign = false) {
        const query = qs({path, presign});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(ref)}/objects?` + query, {
            method: 'GET',
        });
        if (response.status !== 200 && response.status !== 206) {
            throw new Error(await extractError(response));
        }

        return response.text()
    }

    async head(repoId, ref, path) {
        const query = qs({path});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(ref)}/objects?` + query, {
            method: 'HEAD',
        });

        if (response.status !== 200 && response.status !== 206) {
            throw new Error(await extractError(response));
        }

        return {
            headers: response.headers,
        }
    }

    async getStat(repoId, ref, path, presign = false) {
        const query = qs({path, presign});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(ref)}/objects/stat?` + query);
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json()
    }
}

class Commits {
    async log(repoId, refId, after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(refId)}/commits?` + query);
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async blame(repoId, refId, path, type) {
        const params = {amount: 1};
        if (type === 'object') {
            params.objects = path
        } else {
            params.prefixes = path
        }
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(refId)}/commits?${qs(params)}`);
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        const data = await response.json();
        if (data.results.length >= 1) {
            return data.results[0] // found a commit object
        }
        return null // no commit modified this
    }

    async get(repoId, commitId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/commits/${encodeURIComponent(commitId)}`);
        if (response.status === 404) {
            throw new NotFoundError(`could not find commit ${commitId}`);
        } else if (response.status !== 200) {
            throw new Error(`could not get commit: ${await extractError(response)}`);
        }
        return response.json();
    }

    async commit(repoId, branchId, message, metadata = {}) {
        const response = await apiRequest(`/repositories/${repoId}/branches/${branchId}/commits`, {
            method: 'POST',
            body: JSON.stringify({message, metadata}),
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }
}

class Refs {

    async changes(repoId, branchId, after, prefix, delimiter, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, prefix, delimiter, amount});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/diff?` + query);
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async diff(repoId, leftRef, rightRef, after, prefix = "", delimiter = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount, delimiter, prefix});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(leftRef)}/diff/${encodeURIComponent(rightRef)}?` + query);
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async merge(repoId, sourceBranch, destinationBranch, strategy = "", message = "", metadata = {}) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/refs/${encodeURIComponent(sourceBranch)}/merge/${encodeURIComponent(destinationBranch)}`, {
            method: 'POST',
            body: JSON.stringify({strategy, message, metadata})
        });

        let resp;
        switch (response.status) {
            case 200:
                return response.json();
            case 409:
                resp = await response.json();
                throw new MergeError(response.statusText, resp.body);
            case 412:
            default:
                throw new Error(await extractError(response));
        }
    }
}

class Actions {

    async listRuns(repoId, branch = "", commit = "", after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({branch, commit, after, amount});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/actions/runs?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list actions runs: ${await extractError(response)}`);
        }
        return response.json();
    }

    async getRun(repoId, runId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/actions/runs/${encodeURIComponent(runId)}`);
        if (response.status !== 200) {
            throw new Error(`could not get actions run: ${await extractError(response)}`);
        }
        return response.json();
    }

    async listRunHooks(repoId, runId, after = "", amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/actions/runs/${encodeURIComponent(runId)}/hooks?` + query);
        if (response.status !== 200) {
            throw new Error(`could not list actions run hooks: ${await extractError(response)}`)
        }
        return response.json();
    }

    async getRunHookOutput(repoId, runId, hookRunId) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/actions/runs/${encodeURIComponent(runId)}/hooks/${encodeURIComponent(hookRunId)}/output`, {
            headers: {"Content-Type": "application/octet-stream"},
        });
        if (response.status !== 200) {
            throw new Error(`could not get actions run hook output: ${await extractError(response)}`);
        }
        return response.text();
    }

}

class Retention {
    async getGCPolicy(repoID) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoID)}/settings/gc_rules`);
        if (response.status === 404) {
            throw new NotFoundError('policy not found')
        }
        if (response.status !== 200) {
            throw new Error(`could not get gc policy: ${await extractError(response)}`);
        }
        return response.json();
    }

    async setGCPolicyPreflight(repoID) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoID)}/gc/rules/set_allowed`);
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
        return response;
    }

    async setGCPolicy(repoID, policy) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoID)}/settings/gc_rules`, {
            method: 'PUT',
            body: policy
        });
        if (response.status !== 204) {
            throw new Error(`could not set gc policy: ${await extractError(response)}`);
        }
        return response;
    }

    async deleteGCPolicy(repoID) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoID)}/settings/gc_rules`, {
            method: 'DELETE',
        });
        if (response.status !== 204) {
            throw new Error(`could not delete gc policy: ${await extractError(response)}`);
        }
        return response;
    }
}

class Setup {
    async getState() {
        const response = await apiRequest('/setup_lakefs', {
            method: 'GET',
        });
        switch (response.status) {
            case 200:
                return response.json();
            default:
                throw new Error(`Could not get setup state: ${await extractError(response)}`);
        }
    }

    async lakeFS(username) {
        const response = await apiRequest('/setup_lakefs', {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({username: username}),
        });
        switch (response.status) {
            case 200:
                return response.json();
            case 409:
                throw new Error('Setup is already complete.');
            default:
                throw new Error('Unknown');
        }
    }

    async commPrefs(email, updates, security) {
        const response = await apiRequest('/setup_comm_prefs', {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                email,
                featureUpdates: updates,
                securityUpdates: security,
            }),
        });

        switch (response.status) {
            case 200:
                return;
            case 409:
                throw new Error('Setup is already complete.');
            default:
                throw new Error('Unknown');
        }
    }
}

class Config {
    async getStorageConfig() {
        const response = await apiRequest('/config', {
            method: 'GET',
        });
        let cfg, storageCfg;
        switch (response.status) {
            case 200:
                cfg = await response.json();
                storageCfg = cfg.storage_config
                storageCfg.warnings = []
                if (storageCfg.blockstore_type === 'mem') {
                    storageCfg.warnings.push(`Block adapter ${storageCfg.blockstore_type} not usable in production`)
                }
                return storageCfg;
            case 409:
                throw new Error('Conflict');
            default:
                throw new Error('Unknown');
        }
    }

    async getLakeFSVersion() {
        const response = await apiRequest('/config', {
            method: 'GET',
        });
        let cfg;
        switch (response.status) {
            case 200:
                cfg = await response.json();
                return cfg.version_config
            default:
                throw new Error('Unknown');
        }
    }
}

class BranchProtectionRules {
    async getRules(repoID) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoID)}/settings/branch_protection`);
        if (response.status === 404) {
            throw new NotFoundError('branch protection rules not found')
        }
        if (response.status !== 200) {
            throw new Error(`could not get branch protection rules: ${await extractError(response)}`);
        }
        return {
            'checksum': response.headers.get('ETag'),
            'rules': await response.json()
        }
    }

    async createRulePreflight(repoID) {
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoID)}/branch_protection/set_allowed`);
        return response.status === 204;

    }

    async setRules(repoID, rules, lastKnownChecksum) {
        const additionalHeaders = {}
        if (lastKnownChecksum) {
            additionalHeaders['If-Match'] = lastKnownChecksum
        }
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoID)}/settings/branch_protection`, {
            method: 'PUT',
            body: JSON.stringify(rules),
        }, additionalHeaders);
        if (response.status !== 204) {
            throw new Error(`could not create protection rule: ${await extractError(response)}`);
        }
    }
}

class Statistics {
    async postStatsEvents(statsEvents) {
        const request = {
            "events": statsEvents,
        }
        const response = await apiRequest(`/statistics`, {
            method: 'POST',
            body: JSON.stringify(request),
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }
}

class Staging {
    async get(repoId, branchId, path, presign = false) {
        const query = qs({path, presign});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/staging/backing?` + query, {
            method: 'GET'
        });
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async link(repoId, branchId, path, staging, checksum, sizeBytes, contentType = 'application/octet-stream') {
        const query = qs({path});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/staging/backing?` + query, {
            method: 'PUT',
            body: JSON.stringify({
                staging: staging,
                checksum: checksum,
                size_bytes: sizeBytes,
                content_type: contentType
            })
        });
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }
}

class Import {

    async get(repoId, branchId, importId) {
        const query = qs({id: importId});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/import?` + query);
        if (response.status === 404) {
            throw new NotFoundError(`could not find import ${importId}`);
        } else if (response.status !== 200) {
            throw new Error(`could not get import status: ${await extractError(response)}`);
        }
        return response.json();
    }

    async create(repoId, branchId, source, prepend, commitMessage, commitMetadata = {}) {
        const body = {
            "paths": [
                {
                    "path": source,
                    "destination": prepend,
                    "type": "common_prefix",
                }],
            "commit": {
                "message": commitMessage
            },
        };
        if (Object.keys(commitMetadata).length > 0) {
            body.commit["metadata"] = commitMetadata
        }

        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/import`, {
            method: 'POST',
            body: JSON.stringify(body),
        });
        if (response.status !== 202) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async delete(repoId, branchId, importId) {
        const query = qs({id: importId});
        const response = await apiRequest(`/repositories/${encodeURIComponent(repoId)}/branches/${encodeURIComponent(branchId)}/import?` + query, {
            method: 'DELETE',
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }
}

export const repositories = new Repositories();
export const branches = new Branches();
export const tags = new Tags();
export const pulls = new Pulls();
export const objects = new Objects();
export const commits = new Commits();
export const refs = new Refs();
export const setup = new Setup();
export const auth = new Auth();
export const actions = new Actions();
export const retention = new Retention();
export const config = new Config();
export const branchProtectionRules = new BranchProtectionRules();
export const statistics = new Statistics();
export const staging = new Staging();
export const imports = new Import();
