import React from 'react';
import {Switch, Route, useHistory, Redirect} from "react-router-dom";

import RepositoryExplorerPage from './RepositoryExplorerPage';
import {RepositoryListPage} from './RepositoryListPage';
import {AuthManagementPage} from './AuthManagementPage';


export const IndexPage = () => {
    const history = useHistory();
    const currentPath = history.location.pathname;
    const onNavigate = (k) => { history.push(k); };

    return (
        <Switch>
            <Route exact path={"/"}>
                <Redirect to="/repositories"/>
            </Route>
            <Route path="/repositories/:repoId">
                <RepositoryExplorerPage currentTab={currentPath} onNavigate={onNavigate}/>
            </Route>
            <Route path="/auth">
                <AuthManagementPage/>
            </Route>
            <Route path="/repositories">
                <RepositoryListPage/>
            </Route>
        </Switch>
    );
};
