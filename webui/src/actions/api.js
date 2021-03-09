import {isValidBranchName} from "../model/validation";

export const API_ENDPOINT = '/api/v1';
export const DEFAULT_LISTING_AMOUNT = 100;

export const linkToPath = (repoId, branchId, path) => {
    const query = qs({
        path: path,
    });
    return `${API_ENDPOINT}/repositories/${repoId}/refs/${branchId}/objects?${query}`
};

const json =(data) => {
    return JSON.stringify(data, null, "");
};

const qs = (queryParts) => {
    const parts = Object.keys(queryParts).map(key => [key, queryParts[key]]);
    return new URLSearchParams(parts).toString();
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

const apiRequest = async (uri, requestData = {}, additionalHeaders = {}, defaultHeaders = {
    "Accept": "application/json",
    "Content-Type": "application/json",
}) => {
    const response = await fetch(`${API_ENDPOINT}${uri}`, {
        headers: new Headers({
            ...defaultHeaders,
            ...additionalHeaders,
        }),
        ...requestData,
    });
    if (response.status === 401) {
        // unauthorized status code will throw the current user and reload
        localStorage.removeItem('user');
        window.location.reload(true);
    }
    return response;
};

// helper errors
export class NotFoundError extends Error {
    constructor(message) {
        super(message);
        this.name = "NotFoundError";
    }
}

export class MergeError extends Error {
  constructor(message, payload) {
    super(message);
    this.name = "MergeError";
    this.payload = payload;
  }
}


// actual actions:
class Auth {

    async login(accessKeyId, secretAccessKey) {
        const response = await fetch('/auth/login', {
            headers: new Headers({'Content-Type': 'application/json'}),
            method: 'POST',
            body: json({access_key_id: accessKeyId, secret_access_key: secretAccessKey})
        });

        if (response.status === 401) {
            throw new Error('invalid credentials');
        }
        if (response.status !== 200) {
            throw new Error('unknown authentication error');
        }

        const userResponse = await apiRequest('/user');
        const body = await userResponse.json();
        return body.user;
    }

    async listUsers(after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/users?${query}`)
        if (response.status !== 200) {
            throw new Error(`could not list users: ${await extractError(response)}`)
        }
        return response.json();
    }

    async createUser(userId) {
        const response = await apiRequest(`/auth/users`, {method: 'POST', body: json({id: userId})});
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async listGroups(after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/groups?${query}`)
        if (response.status !== 200) {
            throw new Error(`could not list groups: ${await extractError(response)}`)
        }
        return response.json();
    }

    async listGroupMembers(groupId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/groups/${groupId}/members?${query}`)
        if (response.status !== 200) {
            throw new Error(`could not list group members: ${await extractError(response)}`)
        }
        return response.json();
    }

    async addUserToGroup(userId, groupId) {
        const response = await apiRequest(`/auth/groups/${groupId}/members/${userId}`, {method: 'PUT'})
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
    }

    async removeUserFromGroup(userId, groupId) {
        const response = await apiRequest(`/auth/groups/${groupId}/members/${userId}`, {method: 'DELETE'})
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async attachPolicyToUser(userId, policyId) {
        const response = await apiRequest(`/auth/users/${userId}/policies/${policyId}`, {method: 'PUT'})
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
    }

    async detachPolicyFromUser(userId, policyId) {
        const response = await apiRequest(`/auth/users/${userId}/policies/${policyId}`, {method: 'DELETE'})
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async attachPolicyToGroup(groupId, policyId) {
        const response = await apiRequest(`/auth/groups/${groupId}/policies/${policyId}`, {method: 'PUT'})
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
    }

    async detachPolicyFromGroup(groupId, policyId) {
        const response = await apiRequest(`/auth/groups/${groupId}/policies/${policyId}`, {method: 'DELETE'})
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async deleteCredentials(userId, accessKeyId) {
        const response = await apiRequest(`/auth/users/${userId}/credentials/${accessKeyId}`, {method: 'DELETE'})
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async createGroup(groupId) {
        const response = await apiRequest(`/auth/groups`, {method: 'POST',  body: json({id: groupId})});
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async listPolicies(after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/policies?${query}`)
        if (response.status !== 200) {
            throw new Error(`could not list policies: ${await extractError(response)}`)
        }
        return response.json();
    }

    async createPolicy(policyId, policyDocument) {
        const policy = {id: policyId, ...JSON.parse(policyDocument)};
        const response = await apiRequest(`/auth/policies`, {
            method: 'POST',
            body: json(policy)
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async editPolicy(policyId, policyDocument) {
        const policy = {id: policyId, ...JSON.parse(policyDocument)};
        const response = await apiRequest(`/auth/policies/${policyId}`, {
            method: 'PUT',
            body: json(policy)
        });
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async listCredentials(userId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/users/${userId}/credentials?${query}`)
        if (response.status !== 200) {
            throw new Error(`could not list credentials: ${await extractError(response)}`)
        }
        return response.json();
    }

    async createCredentials(userId) {
        const response = await apiRequest(`/auth/users/${userId}/credentials`, {
            method: 'POST',
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async listUserGroups(userId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/auth/users/${userId}/groups?${query}`)
        if (response.status !== 200) {
            throw new Error(`could not list user groups: ${await extractError(response)}`)
        }
        return response.json();
    }

    async listUserPolicies(userId, after, amount = DEFAULT_LISTING_AMOUNT, effective = false) {
        let params = {after, amount};
        if (effective) {
            params.effective =  'true';
        }
        const response = await apiRequest(`/auth/users/${userId}/policies?${qs(params)}`)
        if (response.status !== 200) {
            throw new Error(`could not list policies: ${await extractError(response)}`)
        }
        return response.json();
    }

    async getPolicy(policyId) {
        const response = await apiRequest(`/auth/policies/${policyId}`)
        if (response.status !== 200) {
            throw new Error(`could not get policy: ${await extractError(response)}`)
        }
        return response.json();
    }

    async listGroupPolicies(groupId, after, amount = DEFAULT_LISTING_AMOUNT) {
        let params = {after, amount};
        const response = await apiRequest(`/auth/groups/${groupId}/policies?${qs(params)}`)
        if (response.status !== 200) {
            throw new Error(`could not list policies: ${await extractError(response)}`)
        }
        return response.json();
    }

    async deleteUser(userId) {
        const response = await apiRequest(`/auth/users/${userId}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async deleteUsers (userIds) {
        for (let i = 0; i < userIds.length; i++) {
            const userId = userIds[i];
            await this.deleteUser(userId);
        }

    }

    async deleteGroup(groupId) {
        const response = await apiRequest(`/auth/groups/${groupId}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async deleteGroups (groupIds) {
        for (let i = 0; i < groupIds.length; i++) {
            const groupId = groupIds[i];
            await this.deleteGroup(groupId);
        }
    }

    async deletePolicy(policyId) {
        const response = await apiRequest(`/auth/policies/${policyId}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async deletePolicies (policyIds) {
        for (let i = 0; i < policyIds.length; i++) {
            const policyId = policyIds[i];
            await this.deletePolicy(policyId);
        }
    }

    async logout() {
        const response = await fetch('/auth/logout', {method: 'POST'});
        if (response.status !== 200) {
            throw new Error('unknown authentication error');
        }
    }
}


class Repositories {

    async get(repoId) {
        const response = await apiRequest(`/repositories/${repoId}`);
        if (response.status === 404) {
            throw new NotFoundError(`could not find repository ${repoId}`);
        } else if (response.status !== 200) {
            throw new Error(`could not get repository: ${await extractError(response)}`)
        }
        return response.json();
    }

    async list(after, amount) {
        const query = qs({after, amount});
        const response = await apiRequest(`/repositories?${query}`);
        if (response.status !== 200) {
            throw new Error(`could not list repositories: ${await extractError(response)}`)
        }
        return response.json();
    }

    async filter(from, amount) {
        if (!from) {
            return this.list(from, amount);
        }
        const response = await this.list(from, amount);
        let self;
        try {
            self = await this.get(from);
        } catch (error) {
            if (!(error instanceof NotFoundError)) {
                throw error;
            }
        }
        const results = response.results.filter(repo => repo.id.indexOf(from) === 0);
        if (self) results.unshift(self);
        const hasMore = response.pagination.has_more;

        return {
            results,
            pagination: {
                has_more: hasMore,
                max_per_page: amount,
                results: results.length,
            },
        };
    }

    async create(repo) {
        const response = await apiRequest('/repositories', {
            method: 'POST',
            body: json(repo),
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async delete(repoId) {
        const response = await apiRequest(`/repositories/${repoId}`, {method: 'DELETE'});
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }
}

class Branches {

    async get(repoId, branchId) {
        const response = await apiRequest(`/repositories/${repoId}/branches/${branchId}`);
        if (response.status === 404) {
            throw new NotFoundError(`could not find branch ${branchId}`);
        } else if (response.status !== 200) {
            throw new Error(`could not get branch: ${await extractError(response)}`)
        }
        return response.json();
    }

    async create(repoId, name, source) {
        const response = await apiRequest(`/repositories/${repoId}/branches`, {
            method: 'POST',
            body: json({name, source}),
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async delete(repoId, name) {
        const response = await apiRequest(`/repositories/${repoId}/branches/${name}`, {
            method: 'DELETE',
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async revert(repoId, branch, options) {
        const response = await apiRequest(`/repositories/${repoId}/branches/${branch}`, {
            method: 'PUT',
            body: json(options),
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }

    async list(repoId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/repositories/${repoId}/branches?${query}`);
        if (response.status !== 200) {
            throw new Error(`could not list branches: ${await extractError(response)}`)
        }
        return response.json();
    }


    async filter(repoId, from, amount = DEFAULT_LISTING_AMOUNT) {
        if (!from) {
            return this.list(repoId, from, amount);
        }
        const response = await this.list(repoId, from, amount);
        let self;
        if (isValidBranchName(from)) {
            try {
                self = await this.get(repoId, from);
            } catch (error) {
                if (!(error instanceof NotFoundError)) {
                    throw error;
                }
            }
        }
        const results = response.results.filter(branch => branch.id.indexOf(from) === 0);
        if (self) results.unshift(self);
        const hasMore = response.pagination.has_more;

        return {
            results,
            pagination: {
                has_more: hasMore,
                max_per_page: amount,
                results: results.length,
            },
        };
    }
}

class Objects {

    async list(repoId, ref, tree, after = "", amount = DEFAULT_LISTING_AMOUNT, readUncommitted = true) {
        const query = qs({prefix:tree, amount, after, readUncommitted});
        const response = await apiRequest(`/repositories/${repoId}/refs/${ref}/objects/ls?${query}`);
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async upload(repoId, branchId, path, fileObject) {
        const data = new FormData();
        data.append('content', fileObject);
        const query = qs({path});
        const response = await apiRequest(`/repositories/${repoId}/branches/${branchId}/objects?${query}`, {
            method: 'POST',
            body: data,
        }, {}, {});
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async delete(repoId, branchId, path) {
        const query = qs({path});
        const response = await apiRequest(`/repositories/${repoId}/branches/${branchId}/objects?${query}`, {
            method: 'DELETE',
        });
        if (response.status !== 204) {
            throw new Error(await extractError(response));
        }
    }
}

class Commits {
    async log(repoId, branchId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/repositories/${repoId}/branches/${branchId}/commits?${query}`);
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async commit(repoId, branchId, message, metadata ={}) {
        const response = await apiRequest(`/repositories/${repoId}/branches/${branchId}/commits`, {
            method: 'POST',
            body: json({message, metadata}),
        });
        if (response.status !== 201) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }
}

class Refs {
    async diff(repoId, leftRef, rightRef, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        let response;
        if (leftRef === rightRef) {
            response = await apiRequest(`/repositories/${repoId}/branches/${leftRef}/diff?${query}`);
        } else {
            response = await apiRequest(`/repositories/${repoId}/refs/${leftRef}/diff/${rightRef}?${query}`);
        }
        if (response.status !== 200) {
            throw new Error(await extractError(response));
        }
        return response.json();
    }

    async merge(repoId, sourceBranch, destinationBranch) {
        const response = await apiRequest(`/repositories/${repoId}/refs/${sourceBranch}/merge/${destinationBranch}`, {
            method: 'POST',
            body: '{}',
        });
        switch (response.status) {
            case 200:
              return response.json();
            case 409:
                const resp = await response.json();
                throw new MergeError(response.statusText, resp.body);
            case 412:
            default:
                throw new Error(await extractError(response));
        }
    }
}

class Actions {

    async listRuns(repoId, branch, commit, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({branch, commit, after, amount});
        const response = await apiRequest(`/repositories/${repoId}/actions/runs?${query}`);
        if (response.status !== 200) {
            throw new Error(`could not list actions runs: ${await extractError(response)}`)
        }
        return response.json();
    }

    async getRun(repoId, runId) {
        const response = await apiRequest(`/repositories/${repoId}/actions/runs/${runId}`);
        if (response.status !== 200) {
            throw new Error(`could not get actions run: ${await extractError(response)}`)
        }
        return response.json();
    }

    async listRunHooks(repoId, runId, after, amount = DEFAULT_LISTING_AMOUNT) {
        const query = qs({after, amount});
        const response = await apiRequest(`/repositories/${repoId}/actions/runs/${runId}/hooks?${query}`);
        if (response.status !== 200) {
            throw new Error(`could not list actions run hooks: ${await extractError(response)}`)
        }
        return response.json();
    }

    async getRunHookOutput(repoId, runId, hookRunId) {
        const response = await apiRequest(`/repositories/${repoId}/actions/runs/${runId}/hooks/${hookRunId}/output`, {
            headers: {
                "Content-Type": "application/octet-stream",
            },
        })
        if (response.status !== 200) {
            throw new Error(`could not get actions run hook output: ${await extractError(response)}`)
        }
        return response.text();
    }

}


class Setup {
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
                throw new Error('Conflict');
            default:
                throw new Error('Unknown');
        }
    }
}

class Config {
    async get() {
        const response = await apiRequest('/config', {
            method: 'GET',
        });
        switch (response.status) {
            case 200:
                return response.json();
            case 409:
                throw new Error('Conflict');
            default:
                throw new Error('Unknown');
        }
    }
}


export const repositories = new Repositories();
export const branches = new Branches();
export const objects = new Objects();
export const commits = new Commits();
export const refs = new Refs();
export const setup = new Setup();
export const auth = new Auth();
export const actions = new Actions();
export const config = new Config();
