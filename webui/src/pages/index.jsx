import React, { useEffect } from 'react';

import { BrowserRouter as Router, Routes, Route, Navigate, useLocation } from 'react-router-dom';

// pages
import RepositoriesPage from './repositories';
import DatasetsPage from './datasets';
import { RepositoryPageLayout } from '../lib/components/repository/layout.jsx';
import RepositoryObjectsPage from './repositories/repository/objects';
import RepositoryObjectsViewPage from './repositories/repository/objectViewer';

import RepositoryCommitsPage from './repositories/repository/commits';
import RepositoryCommitPage from './repositories/repository/commits/commit';
import RepositoryBranchesPage from './repositories/repository/branches';
import RepositoryRevertPage from './repositories/repository/branches/revert';
import RepositoryTagsPage from './repositories/repository/tags';
import RepositoryPullsListPage from './repositories/repository/pulls/pullsList';
import RepositoryCreatePullPage from './repositories/repository/pulls/createPull';
import RepositoryPullDetailsPage from './repositories/repository/pulls/pullDetails';
import RepositoryComparePage from './repositories/repository/compare';
import RepositoryActionsPage from './repositories/repository/actions';
import RepositoryGeneralSettingsPage from './repositories/repository/settings/general';
import RepositoryRetentionPage from './repositories/repository/settings/retention';
import RepositorySettingsBranchesPage from './repositories/repository/settings/branches';
import { SettingsLayout } from './repositories/repository/settings/layout';
import Layout from '../lib/components/layout';
import CredentialsPage from './auth/credentials';
import GroupsPage from './auth/groups';
import UsersIndexPage from './auth/users';
import PoliciesPage from './auth/policies';
import LoginPage from './auth/login';
import Setup from './setup';
import { AuthLayout } from '../lib/components/auth/layout';
import RepositoryActionPage from './repositories/repository/actions/run';
import { WithAppContext } from '../lib/hooks/appContext';
import { AuthProvider } from '../lib/auth/authContext';
import RequiresAuth from '../lib/components/requiresAuth';

// Component to handle browser redirection - to exit the React app.
const Redirect = () => {
    const location = useLocation();
    // Break out of React to the actual URL - do not use Navigate.
    useEffect(() => {
        const fullPath = location.pathname + location.search + location.hash;
        window.location.replace(fullPath);
    }, [location.pathname, location.search, location.hash]);

    return <div>Redirecting...</div>;
};

export const IndexPage = () => {
    return (
        <Router>
            <AuthProvider>
                <WithAppContext>
                    <Routes>
                        <Route element={<RequiresAuth />}>
                            <Route index element={<Navigate to="/repositories" />} />
                            <Route path="repositories" element={<Layout />}>
                                <Route index element={<RepositoriesPage />} />
                                <Route path=":repoId" element={<RepositoryPageLayout />}>
                                    <Route path="objects" element={<RepositoryObjectsPage />} />
                                    <Route path="object" element={<RepositoryObjectsViewPage />} />
                                    <Route path="commits">
                                        <Route index element={<RepositoryCommitsPage />} />
                                        <Route path=":commitId" element={<RepositoryCommitPage />} />
                                    </Route>
                                    <Route path="branches">
                                        <Route index element={<RepositoryBranchesPage />} />
                                        <Route path=":branchId/revert" element={<RepositoryRevertPage />} />
                                    </Route>
                                    <Route path="tags" element={<RepositoryTagsPage />} />
                                    <Route path="pulls">
                                        <Route index element={<RepositoryPullsListPage />} />
                                        <Route path="create" element={<RepositoryCreatePullPage />} />
                                        <Route path=":pullId" element={<RepositoryPullDetailsPage />} />
                                    </Route>
                                    <Route path="compare/*" element={<RepositoryComparePage />} />
                                    <Route path="actions">
                                        <Route index element={<RepositoryActionsPage />} />
                                        <Route path=":runId" element={<RepositoryActionPage />} />
                                    </Route>
                                    <Route path="settings" element={<SettingsLayout />}>
                                        <Route index element={<Navigate to="general" />} />
                                        <Route path="general" element={<RepositoryGeneralSettingsPage />} />
                                        <Route path="retention" element={<RepositoryRetentionPage />} />
                                        <Route path="branches" element={<RepositorySettingsBranchesPage />} />
                                    </Route>
                                    <Route index element={<Navigate to="objects" />} />
                                </Route>
                            </Route>
                            <Route path="datasets" element={<Layout />}>
                                <Route index element={<DatasetsPage />} />
                            </Route>
                            <Route path="auth" element={<Layout />}>
                                <Route index element={<Navigate to="credentials" replace />} />
                                <Route element={<AuthLayout />}>
                                    <Route path="credentials" element={<CredentialsPage />} />
                                    <Route path="users" element={<UsersIndexPage />} />
                                    <Route path="groups" element={<GroupsPage />} />
                                    <Route path="policies" element={<PoliciesPage />} />
                                </Route>
                            </Route>
                            <Route path="api/v1/auth/get-token/release-token/*" element={<Redirect />} />
                            <Route path="*" element={<Navigate to="/repositories" replace />} />
                        </Route>
                        <Route path="auth" element={<Layout />}>
                            <Route path="login" element={<LoginPage />} />
                        </Route>
                        <Route path="/setup" element={<Layout />}>
                            <Route index element={<Setup />} />
                            <Route path="*" element={<Setup />} />
                        </Route>
                    </Routes>
                </WithAppContext>
            </AuthProvider>
        </Router>
    );
};
