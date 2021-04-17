import {Redirect, Route, Switch} from "react-router-dom";
import RepositoryObjectsPage from "./objects";
import RepositoryChangesPage from "./changes";
import RepositoryBranchesPage from "./branches";
import RepositoryComparePage from "./compare";
import RepositoryCommitsIndexPage from "./commits";
import RepositoryActionsIndexPage from "./actions";
import RepositorySettingsPage from "./settings";


const RepositoryPage = () => {
    return (
        <Switch>
            <Route exact path="/">
                <Redirect to="/repositories"/>
            </Route>
            <Route path="/repositories/:repoId/objects">
                <RepositoryObjectsPage/>
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
            <Route path="/repositories/:repoId/compare">
                <RepositoryComparePage/>
            </Route>
            <Route path="/repositories/:repoId/actions">
                <RepositoryActionsIndexPage/>
            </Route>
            <Route path="/repositories/:repoId/settings">
                <RepositorySettingsPage/>
            </Route>
        </Switch>
    )
};

export default RepositoryPage;