import React from "react";

import {
    BrowserRouter as Router,
    Routes,
    Route,
    Navigate,
} from "react-router-dom";
import {WithLoginConfigContext} from "../lib/hooks/conf";

// pages
import RepositoriesPage from "./repositories";
import {RepositoryPageLayout} from "../lib/components/repository/layout.jsx";
import RepositoryObjectsPage from "./repositories/repository/objects";
import RepositoryObjectsViewPage from "./repositories/repository/objectViewer";
import RepositoryChangesPage from "./repositories/repository/changes";
import RepositoryCommitsPage from "./repositories/repository/commits";
import RepositoryCommitPage from "./repositories/repository/commits/commit";
import RepositoryBranchesPage from "./repositories/repository/branches";
import RepositoryTagsPage from "./repositories/repository/tags";
import RepositoryPullsPage from "./repositories/repository/pulls/pulls";
import RepositoryCreatePullPage from "./repositories/repository/pulls/createPull";
import RepositoryPullDetailsPage from "./repositories/repository/pulls/pullDetails";
import RepositoryComparePage from "./repositories/repository/compare";
import RepositoryActionsPage from "./repositories/repository/actions";
import RepositoryGeneralSettingsPage from "./repositories/repository/settings/general";
import RepositoryRetentionPage from "./repositories/repository/settings/retention";
import RepositorySettingsBranchesPage from "./repositories/repository/settings/branches";
import {SettingsLayout} from "./repositories/repository/settings/layout";
import Layout from "../lib/components/layout";
import CredentialsPage from "./auth/credentials";
import GroupsPage from "./auth/groups";
import GroupMembersPage from "./auth/groups/group/members";
import GroupPoliciesPage from "./auth/groups/group/policies";
import UsersIndexPage, {UsersPage} from "./auth/users";
import UserGroupsPage from "./auth/users/user/groups";
import UserPoliciesPage from "./auth/users/user/policies";
import UserEffectivePoliciesPage from "./auth/users/user/effectivePolicies";
import UserCredentialsPage from "./auth/users/user/credentials";
import PoliciesPage from "./auth/policies";
import PolicyPage from "./auth/policies/policy";
import LoginPage from "./auth/login";
import ActivateInvitedUserPage from "./auth/users/create-user-with-password";
import Setup from "./setup";
import {AuthLayout} from "../lib/components/auth/layout";
import RepositoryActionPage from "./repositories/repository/actions/run";
import {WithAppContext} from "../lib/hooks/appContext";

export const IndexPage = () => {
    return (
        <Router>
            <WithAppContext>
                <WithLoginConfigContext>
                    <Routes>
                        <Route index element={<Navigate to="/repositories"/>}/>
                        <Route path="repositories" element={<Layout logged={true}/>}>
                            <Route index element={<RepositoriesPage/>}/>
                            <Route path=":repoId" element={<RepositoryPageLayout/>}>
                                <Route path="objects" element={<RepositoryObjectsPage/>}/>
                                <Route path="object" element={<RepositoryObjectsViewPage/>}/>
                                <Route path="changes" element={<RepositoryChangesPage/>}/>
                                <Route path="commits">
                                    <Route index element={<RepositoryCommitsPage/>}/>
                                    <Route path=":commitId" element={<RepositoryCommitPage/>}/>
                                </Route>
                                <Route path="branches" element={<RepositoryBranchesPage/>}/>
                                <Route path="tags" element={<RepositoryTagsPage/>}/>
                                <Route path="pulls">
                                    <Route index element={<RepositoryPullsPage/>}/>
                                    <Route path="create" element={<RepositoryCreatePullPage/>}/>
                                    <Route path=":pullId" element={<RepositoryPullDetailsPage/>}/>
                                </Route>
                                <Route path="compare/*" element={<RepositoryComparePage/>}/>
                                <Route path="actions">
                                    <Route index element={<RepositoryActionsPage/>}/>
                                    <Route path=":runId" element={<RepositoryActionPage/>}/>
                                </Route>
                                <Route path="settings" element={<SettingsLayout/>}>
                                    <Route index element={<Navigate to="general"/>}/>
                                    <Route path="general" element={<RepositoryGeneralSettingsPage/>}/>
                                    <Route path="retention" element={<RepositoryRetentionPage/>}/>
                                    <Route path="branches" element={<RepositorySettingsBranchesPage/>}/>
                                </Route>
                                <Route index element={<Navigate to="objects"/>}/>
                            </Route>
                        </Route>
                        <Route path="auth" element={<Layout logged={false}/>}>
                            <Route index element={<Navigate to="credentials"/>}/>
                            <Route path="login" element={<LoginPage/>}/>
                            <Route path="users/create" element={<ActivateInvitedUserPage/>}/>
                            <Route element={<AuthLayout/>}>
                                <Route path="credentials" element={<CredentialsPage/>}/>
                                <Route path="users" element={<UsersIndexPage/>}>
                                    <Route index element={<UsersPage/>}/>
                                    <Route path=":userId">
                                        <Route index element={<Navigate to="groups"/>}/>
                                        <Route path="groups" element={<UserGroupsPage/>}/>
                                        <Route exact path="policies" element={<UserPoliciesPage/>}/>
                                        <Route exact path="policies/effective" element={<UserEffectivePoliciesPage/>}/>
                                        <Route exact path="credentials" element={<UserCredentialsPage/>}/>
                                    </Route>
                                </Route>
                                <Route path="groups">
                                    <Route index element={<GroupsPage/>}/>
                                    <Route path=":groupId">
                                        <Route index element={<Navigate to="members"/>}/>
                                        <Route path="members" element={<GroupMembersPage/>}/>
                                        <Route path="policies" element={<GroupPoliciesPage/>}/>
                                    </Route>
                                </Route>
                                <Route path="policies">
                                    <Route index element={<PoliciesPage/>}/>
                                    <Route path=":policyId" element={<PolicyPage/>}/>
                                </Route>
                            </Route>
                        </Route>
                        <Route path="/setup" element={<Layout logged={false}/>}>
                            <Route index element={<Setup/>}/>
                            <Route path="*" element={<Setup/>}/>
                        </Route>
                        <Route path="*" element={<Navigate to="/repositories" replace/>}/>
                    </Routes>
                </WithLoginConfigContext>
            </WithAppContext>
        </Router>
    );
};
