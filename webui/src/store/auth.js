import {
    AUTH_LOGIN,
    AUTH_LOGOUT,
    AUTH_REDIRECTED,
    AUTH_LIST_USERS,
    AUTH_CREATE_USER,
    AUTH_LIST_GROUPS,
    AUTH_CREATE_GROUP,
    AUTH_GET_POLICY,
    AUTH_LIST_POLICIES,
    AUTH_CREATE_POLICY,
    AUTH_LIST_CREDENTIALS,
    AUTH_CREATE_CREDENTIALS,
    AUTH_LIST_USER_GROUPS,
    AUTH_LIST_USER_POLICIES,
    AUTH_LIST_USER_EFFECTIVE_POLICIES,
    AUTH_LIST_GROUP_POLICIES,
    AUTH_LIST_GROUP_MEMBERS,
    AUTH_ADD_USER_TO_GROUP,
    AUTH_REMOVE_USER_FROM_GROUP,
    AUTH_ATTACH_POLICY_TO_USER,
    AUTH_DETACH_POLICY_FROM_USER,
    AUTH_DELETE_CREDENTIALS,
    AUTH_DELETE_GROUPS,
    AUTH_DELETE_POLICIES,
    AUTH_DELETE_USERS, AUTH_ATTACH_POLICY_TO_GROUP, AUTH_DETACH_POLICY_FROM_GROUP, AUTH_EDIT_POLICY,
} from '../actions/auth';

import * as async from "./async";

const initialState = {
    user: null,
    loginError: null,
    redirectTo: null,
    usersList: async.initialState,
    userCreation: async.actionInitialState,
    userDeletion: async.actionInitialState,
    groupsList: async.initialState,
    groupMemberList: async.initialState,
    groupMembershipCreation: async.actionInitialState,
    groupMembershipDeletion: async.actionInitialState,
    groupCreation: async.actionInitialState,
    groupDeletion: async.actionInitialState,
    policy: async.initialState,
    policiesList: async.initialState,
    policyCreation: async.actionInitialState,
    policyUserAttachment: async.actionInitialState,
    policyUserDetachment: async.actionInitialState,
    policyGroupAttachment: async.actionInitialState,
    policyGroupDetachment: async.actionInitialState,
    policyDeletion: async.actionInitialState,
    policyEdit: async.actionInitialState,
    credentialsList: async.initialState,
    credentialsCreation: async.actionInitialState,
    credentialsDeletion: async.actionInitialState,
    userGroupsList: async.initialState,
    userPoliciesList: async.initialState,
    userEffectivePoliciesList: async.initialState,
    groupPoliciesList: async.initialState,
};

const store = (state = initialState, action) => {
    state = {
        ...state,
        usersList: async.reduce(AUTH_LIST_USERS, state.usersList, action),
        userCreation: async.actionReduce(AUTH_CREATE_USER, state.userCreation, action),
        userDeletion: async.actionReduce(AUTH_DELETE_USERS, state.userDeletion, action),
        groupsList: async.reduce(AUTH_LIST_GROUPS, state.groupsList, action),
        groupMemberList: async.reduce(AUTH_LIST_GROUP_MEMBERS, state.groupMemberList, action),
        groupMembershipCreation: async.actionReduce(AUTH_ADD_USER_TO_GROUP, state.groupMembershipCreation, action),
        groupMembershipDeletion: async.actionReduce(AUTH_REMOVE_USER_FROM_GROUP, state.groupMembershipDeletion, action),
        groupCreation: async.actionReduce(AUTH_CREATE_GROUP, state.groupCreation, action),
        groupDeletion: async.actionReduce(AUTH_DELETE_GROUPS, state.groupDeletion, action),
        policy: async.reduce(AUTH_GET_POLICY, state.policy, action),
        policiesList: async.reduce(AUTH_LIST_POLICIES, state.policiesList, action),
        policyCreation: async.actionReduce(AUTH_CREATE_POLICY, state.policyCreation, action),
        policyEdit: async.actionReduce(AUTH_EDIT_POLICY, state.policyEdit, action),
        policyUserAttachment: async.actionReduce(AUTH_ATTACH_POLICY_TO_USER, state.policyUserAttachment, action),
        policyUserDetachment: async.actionReduce(AUTH_DETACH_POLICY_FROM_USER, state.policyUserDetachment, action),
        policyGroupAttachment: async.actionReduce(AUTH_ATTACH_POLICY_TO_GROUP, state.policyGroupAttachment, action),
        policyGroupDetachment: async.actionReduce(AUTH_DETACH_POLICY_FROM_GROUP, state.policyGroupDetachment, action),
        policyDeletion: async.actionReduce(AUTH_DELETE_POLICIES, state.policyDeletion, action),
        credentialsList: async.reduce(AUTH_LIST_CREDENTIALS, state.credentialsList, action),
        credentialsCreation: async.actionReduce(AUTH_CREATE_CREDENTIALS, state.credentialsCreation, action),
        credentialsDeletion: async.actionReduce(AUTH_DELETE_CREDENTIALS, state.credentialsDeletion, action),
        userGroupsList: async.reduce(AUTH_LIST_USER_GROUPS, state.userGroupsList, action),
        userPoliciesList: async.reduce(AUTH_LIST_USER_POLICIES, state.userPoliciesList, action),
        userEffectivePoliciesList: async.reduce(AUTH_LIST_USER_EFFECTIVE_POLICIES, state.userEffectivePoliciesList, action),
        groupPoliciesList: async.reduce(AUTH_LIST_GROUP_POLICIES, state.groupPoliciesList, action)
    }

    switch (action.type) {
        case AUTH_LOGIN.error:
            return {
                ...state,
                user: null,
                loginError: action.error,
            };
        case AUTH_LOGIN.success:
            return {
                ...state,
                user: {...action.payload.user, accessKeyId: action.payload.accessKeyId},
                redirectTo: action.payload.redirectTo,
                loginError: null,
            };
        case AUTH_LOGOUT.success:
            return {
                ...initialState,
                user: null,
            };
        case AUTH_REDIRECTED:
            return {
                ...state,
                redirectTo: null,
            };
        default:
            return state;
    }
};

export default store;
