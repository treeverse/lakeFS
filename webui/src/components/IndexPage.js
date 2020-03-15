import React from 'react';
import {Switch, Route, useHistory} from "react-router-dom";

import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";

import RepositoryExplorerPage from './RepositoryExplorerPage';
import {RepositoryListPage} from './RepositoryListPage';


const IndexPageTabs = ({ currentScreen, onNavigate }) => {
    return (
        <Tabs activeKey={currentScreen} onSelect={onNavigate} className={"mt-5"}>
            <Tab eventKey="/" title="Repositories">
                <RepositoryListPage/>
            </Tab>
        </Tabs>
    );
};

export const IndexPage = () => {
    const history = useHistory();
    const currentPath = history.location.pathname;

    return (
        <Switch>
            <Route path="/repositories/:repoId">
                <RepositoryExplorerPage currentTab={currentPath} onNavigate={(k) => { history.push(k); }}/>
            </Route>

            <Route exact path="/">
                <IndexPageTabs currentScreen={currentPath} onNavigate={(k) => { history.push(k); }}/>
            </Route>
        </Switch>
    );
};
