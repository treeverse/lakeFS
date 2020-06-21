import * as api from "./api";
import {AsyncActionType} from "./request";
import {DEFAULT_LISTING_AMOUNT} from "./api";


export const
    AUTH_LOGIN = new AsyncActionType('AUTH_LOGIN'),
    AUTH_LOGOUT = new AsyncActionType('AUTH_LOGOUT'),
    AUTH_LIST_USERS = new AsyncActionType('AUTH_LIST_USERS'),
    AUTH_CREATE_USER = new AsyncActionType('AUTH_CREATE_USER'),
    AUTH_LIST_GROUPS = new AsyncActionType('AUTH_LIST_GROUPS'),
    AUTH_CREATE_GROUP = new AsyncActionType('AUTH_CREATE_GROUP'),
    AUTH_LIST_POLICIES = new AsyncActionType('AUTH_LIST_POLICIES'),
    AUTH_CREATE_POLICY = new AsyncActionType('AUTH_CREATE_POLICY'),
    AUTH_EDIT_POLICY = new AsyncActionType('AUTH_EDIT_POLICY'),
    AUTH_LIST_CREDENTIALS = new AsyncActionType('AUTH_LIST_CREDENTIALS'),
    AUTH_CREATE_CREDENTIALS = new AsyncActionType('AUTH_CREATE_CREDENTIALS'),
    AUTH_DELETE_CREDENTIALS = new AsyncActionType('AUTH_DELETE_CREDENTIALS'),
    AUTH_LIST_USER_GROUPS = new AsyncActionType('AUTH_LIST_USER_GROUPS'),
    AUTH_ADD_USER_TO_GROUP = new AsyncActionType('AUTH_ADD_USER_TO_GROUP'),
    AUTH_REMOVE_USER_FROM_GROUP = new AsyncActionType('AUTH_REMOVE_USER_FROM_GROUP'),
    AUTH_LIST_USER_POLICIES = new AsyncActionType('AUTH_LIST_USER_POLICIES'),
    AUTH_LIST_USER_EFFECTIVE_POLICIES = new AsyncActionType('AUTH_LIST_USER_EFFECTIVE_POLICIES'),
    AUTH_LIST_GROUP_POLICIES = new AsyncActionType('AUTH_LIST_GROUP_POLICIES'),
    AUTH_LIST_GROUP_MEMBERS = new AsyncActionType('AUTH_LIST_GROUP_MEMBERS'),
    AUTH_GET_POLICY = new AsyncActionType('AUTH_GET_POLICY'),
    AUTH_ATTACH_POLICY_TO_USER = new AsyncActionType('AUTH_ATTACH_POLICY_TO_USER'),
    AUTH_DETACH_POLICY_FROM_USER = new AsyncActionType('AUTH_DETACH_POLICY_FROM_USER'),
    AUTH_ATTACH_POLICY_TO_GROUP = new AsyncActionType('AUTH_ATTACH_POLICY_TO_GROUP'),
    AUTH_DETACH_POLICY_FROM_GROUP = new AsyncActionType('AUTH_DETACH_POLICY_FROM_GROUP'),
    AUTH_DELETE_USERS = new AsyncActionType('AUTH_DELETE_USERS'),
    AUTH_DELETE_GROUPS = new AsyncActionType('AUTH_DELETE_GROUPS'),
    AUTH_DELETE_POLICIES = new AsyncActionType('AUTH_DELETE_POLICIES'),
    AUTH_REDIRECTED = 'AUTH_REDIRECTED';

export const logout = () => {
    return AUTH_LOGOUT.execute(async () => {
        return {};
    });
};

export const listUsers = (after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_USERS.execute(async () => {
        return await api.auth.listUsers(after, amount);
    });
};

export const createUser = (userId) => {
    return AUTH_CREATE_USER.execute(async () => {
        return await api.auth.createUser(userId);
    });
};

export const resetCreateUser = () => ({
    ...AUTH_CREATE_USER.resetAction(),
});


export const listGroups = (after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_GROUPS.execute(async () => {
        return await api.auth.listGroups(after, amount);
    });
};


export const createGroup = (groupId) => {
    return AUTH_CREATE_GROUP.execute(async () => {
        return await api.auth.createGroup(groupId);
    });
};

export const resetCreateGroup = () => ({
    ...AUTH_CREATE_GROUP.resetAction(),
});


export const listPolicies = (after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_POLICIES.execute(async () => {
        return await api.auth.listPolicies(after, amount);
    });
};

export const createPolicy = (policyId, policyDocument) => {
    return AUTH_CREATE_POLICY.execute(async () => {
        return await api.auth.createPolicy(policyId, policyDocument);
    });
};

export const resetCreatePolicy = () => ({
    ...AUTH_CREATE_POLICY.resetAction(),
});

export const editPolicy = (policyId, policyDocument) => {
    return AUTH_EDIT_POLICY.execute(async () => {
        return await api.auth.editPolicy(policyId, policyDocument);
    });
};

export const resetEditPolicy = () => ({
    ...AUTH_EDIT_POLICY.resetAction(),
});

export const listCredentials = (userId, after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_CREDENTIALS.execute(async () => {
        return await api.auth.listCredentials(userId, after, amount);
    });
};

export const createCredentials = (userId) => {
    return AUTH_CREATE_CREDENTIALS.execute(async () => {
        return await api.auth.createCredentials(userId);
    });
};

export const resetCreateCredentials = () => ({
    ...AUTH_CREATE_CREDENTIALS.resetAction()
});

export const deleteCredentials = (userId, accessKeyId) => {
    return AUTH_DELETE_CREDENTIALS.execute(async () => {
        return await api.auth.deleteCredentials(userId, accessKeyId);
    });
};

export const resetDeleteCredentials = () => ({
    ...AUTH_DELETE_CREDENTIALS.resetAction()
});

export const listGroupMembers = (groupId, after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_GROUP_MEMBERS.execute(async () => {
        return await api.auth.listGroupMembers(groupId, after, amount)
    });
}

export const addUserToGroup = (userId, groupId) => {
  return AUTH_ADD_USER_TO_GROUP.execute(async () => {
      return await api.auth.addUserToGroup(userId, groupId)
  });
};

export const resetAddUserToGroup = () => ({
    ...AUTH_ADD_USER_TO_GROUP.resetAction()
})


export const attachPolicyToUser = (userId, policyId) => {
    return AUTH_ATTACH_POLICY_TO_USER.execute(async () => {
        return await api.auth.attachPolicyToUser(userId, policyId)
    });
};

export const resetAttachPolicyToUser = () => ({
    ...AUTH_ATTACH_POLICY_TO_USER.resetAction()
})

export const detachPolicyFromUser = (userId, policyId) => {
    return AUTH_DETACH_POLICY_FROM_USER.execute(async () => {
        return await api.auth.detachPolicyFromUser(userId, policyId)
    });
};

export const resetDetachPolicyFromUser = () => ({
    ...AUTH_DETACH_POLICY_FROM_USER.resetAction()
})

export const attachPolicyToGroup = (groupId, policyId) => {
    return AUTH_ATTACH_POLICY_TO_GROUP.execute(async () => {
        return await api.auth.attachPolicyToGroup(groupId, policyId)
    });
};

export const resetAttachPolicyToGroup = () => ({
    ...AUTH_ATTACH_POLICY_TO_GROUP.resetAction()
})

export const detachPolicyFromGroup = (groupId, policyId) => {
    return AUTH_DETACH_POLICY_FROM_GROUP.execute(async () => {
        return await api.auth.detachPolicyFromGroup(groupId, policyId)
    });
};

export const resetDetachPolicyFromGroup = () => ({
    ...AUTH_DETACH_POLICY_FROM_GROUP.resetAction()
})

export const removeUserFromGroup = (userId, groupId) => {
    return AUTH_REMOVE_USER_FROM_GROUP.execute(async () => {
        return await api.auth.removeUserFromGroup(userId, groupId)
    });
};

export const resetRemoveUserFromGroup = () => ({
    ...AUTH_REMOVE_USER_FROM_GROUP.resetAction()
});

export const listUserGroups = (userId, after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_USER_GROUPS.execute(async () => {
        return await api.auth.listUserGroups(userId, after, amount);
    });
};

export const listUserPolicies = (userId, after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_USER_POLICIES.execute(async () => {
        return await api.auth.listUserPolicies(userId, after, amount, false);
    });
};

export const listUserEffectivePolicies = (userId, after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_USER_EFFECTIVE_POLICIES.execute(async () => {
        return await api.auth.listUserPolicies(userId, after, amount, true);
    });
};

export const listGroupPolicies = (groupId, after = "", amount = DEFAULT_LISTING_AMOUNT) => {
    return AUTH_LIST_GROUP_POLICIES.execute(async () => {
        return await api.auth.listGroupPolicies(groupId, after, amount);
    });
};

export const getPolicy = (policyId) => {
    return AUTH_GET_POLICY.execute(async () => {
        return await api.auth.getPolicy(policyId);
    })
};

export const deleteUsers = (userIds) => {
    return AUTH_DELETE_USERS.execute(async () => {
        return await api.auth.deleteUsers(userIds);
    });
};

export const resetDeleteUsers = () => ({
    ...AUTH_DELETE_USERS.resetAction(),
});

export const deleteGroups = (groupIds) => {
    return AUTH_DELETE_GROUPS.execute(async () => {
        return await api.auth.deleteGroups(groupIds);
    });
};

export const resetDeleteGroups = () => ({
    ...AUTH_DELETE_GROUPS.resetAction(),
});

export const deletePolicies = (policyIds) => {
    return AUTH_DELETE_POLICIES.execute(async () => {
        return await api.auth.deletePolicies(policyIds);
    });
};

export const resetDeletePolicies = () => ({
    ...AUTH_DELETE_POLICIES.resetAction(),
});

export const redirected = () => ({
    type: AUTH_REDIRECTED,
});


export const login = (accessKeyId, secretAccessKey, redirectedUrl) => {
    return AUTH_LOGIN.execute(async () => {
        const response =  await api.auth.login(accessKeyId, secretAccessKey);
        return {
            user: response,
            redirectTo: redirectedUrl,
            accessKeyId,
        };
    })
};