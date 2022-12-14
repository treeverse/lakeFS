import React from "react";

import {Redirect, Route, Switch} from "react-router-dom";
import RepositoryObjectsPage from "./objects";
import RepositoryChangesPage from "./changes";
import RepositoryBranchesPage from "./branches";
import RepositoryTagsPage from "./tags";
import RepositoryComparePage from "./compare";
import RepositoryCommitsIndexPage from "./commits";
import RepositoryActionsIndexPage from "./actions";
import RepositoryGeneralSettingsPage from "./settings/general";
import RepositoryRetentionPage from "./settings/retention";
import RepositorySettingsBranchesPage from "./settings/branches";
import RepositoryObjectsViewPage from "./objectViewer";


const RepositoryPage = () => {
    return (
        <Switch>
            <Route exact path="/">
                <Redirect to="/repositories"/>
            </Route>
            <Route exact path="/repositories/:repoId/objects">
                <RepositoryObjectsPage/>
            </Route>
            <Route path="/repositories/:repoId/object">
                <RepositoryObjectsViewPage />
            </Route>
            <Route path="/repositories/:repoId/changes">
                <RepositoryChangesPage/>
            </Route>
            <Route path="/repositories/:repoId/commits">
                <RepositoryCommitsIndexPage/>
            </Route>
            <Route path="/repositories/:repoId/branches">
                <RepositoryBranchesPage/>
            </Route>
            <Route path="/repositories/:repoId/tags">
                <RepositoryTagsPage/>
            </Route>
            <Route path="/repositories/:repoId/compare">
                <RepositoryComparePage/>
            </Route>
            <Route path="/repositories/:repoId/actions">
                <RepositoryActionsIndexPage/>
            </Route>
            <Route exact path="/repositories/:repoId/settings/">
                <RepositoryGeneralSettingsPage/>
            </Route>
            <Route path="/repositories/:repoId/settings/general">
                <RepositoryGeneralSettingsPage/>
            </Route>
            <Route path="/repositories/:repoId/settings/retention">
                <RepositoryRetentionPage/>
            </Route>
            <Route path="/repositories/:repoId/settings/branches">
                <RepositorySettingsBranchesPage/>
            </Route>
            <Redirect from="/repositories/:repoId" to="/repositories/:repoId/objects" />
        </Switch>
    )
};

export default RepositoryPage;
