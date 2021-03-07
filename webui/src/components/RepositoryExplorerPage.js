import React, {useEffect} from "react";
import {
    useParams,
    useLocation,
    Switch,
    Route,
    useRouteMatch,
    Link,
    generatePath,
    Redirect,
} from "react-router-dom";

import Breadcrumb from "react-bootstrap/Breadcrumb";
import {FileDiffIcon, GitCommitIcon, DatabaseIcon, GitBranchIcon, GitCompareIcon, SettingsIcon, PlayIcon} from "@primer/octicons-react";

import TreePage from './TreePage';
import ChangesPage from './ChangesPage';
import CommitsPage from './CommitsPage';
import {connect} from "react-redux";
import {getRepository} from "../actions/repositories";
import Nav from "react-bootstrap/Nav";
import Alert from "react-bootstrap/Alert";
import BranchesPage from "./BranchesPage";
import ComparePage from "./ComparePage";
import {ActionsRunsPage, ActionsRunPage} from "./ActionsPage";
import RepoSettingsPage from "./RepoSettingsPage";


function useQuery() {
    return new URLSearchParams(useLocation().search);
}

const qs = (queryParts) => {
    const parts = Object.keys(queryParts).map(key => [key, queryParts[key]]);
    const str = new URLSearchParams(parts).toString();
    if (str.length > 0) {
        return `?${str}`;
    }
    return str;
};

const RoutedTab = ({ passInQuery = [], url, children }) => {
    const urlParams = useParams();
    const queryParams = useQuery();

    const queryString = {};

    passInQuery.forEach(param => {
       if (queryParams.has(param)) queryString[param] = queryParams.get(param);
    });


    const address = `${generatePath(url, urlParams)}${qs(queryString)}`;
    const active = useRouteMatch(url);

    return <Nav.Link as={Link} to={address} active={active}>{children}</Nav.Link>
};

const RepositoryTabs = () => {
    return (
        <Nav variant="tabs" defaultActiveKey="/home">
            <Nav.Item>
                <RoutedTab url="/repositories/:repoId/tree" passInQuery={['branch', 'commit']}><DatabaseIcon/>  Objects</RoutedTab>
            </Nav.Item>
            <Nav.Item>
                <RoutedTab url="/repositories/:repoId/changes" passInQuery={['branch']}><FileDiffIcon/>  Changes</RoutedTab>
            </Nav.Item>
            <Nav.Item>
                <RoutedTab url="/repositories/:repoId/commits" passInQuery={['branch']}><GitCommitIcon/>  Commits</RoutedTab>
            </Nav.Item>
            <Nav.Item>
                <RoutedTab url="/repositories/:repoId/branches"><GitBranchIcon/>  Branches</RoutedTab>
            </Nav.Item>
            <Nav.Item>
                <RoutedTab url="/repositories/:repoId/compare" passInQuery={['branch']}><GitCompareIcon/>  Compare</RoutedTab>
            </Nav.Item>
            <Nav.Item>
                <RoutedTab url="/repositories/:repoId/actions" passInQuery={['branch']}><PlayIcon/>  Actions</RoutedTab>
            </Nav.Item>
            <Nav.Item>
                <RoutedTab url="/repositories/:repoId/settings"><SettingsIcon/>  Settings</RoutedTab>
            </Nav.Item>
        </Nav>
    );
};

const RepositoryExplorerPage = ({ repo, getRepository }) => {
    const { repoId } = useParams();
    const query = useQuery();

    useEffect(() => {
        getRepository(repoId);
    }, [getRepository, repoId]);


    if (repo.loading) {
        return (
            <div className="mt-5">
                <p>Loading...</p>
            </div>
        );
    }

    if (!!repo.error) {
        return (
            <div className="mt-5">
                <Alert variant="danger">{repo.error}</Alert>
            </div>

        );
    }

    // we have a repo object
    const branchId = query.get('branch');
    const branch = (!!branchId) ? branchId : ((!!repo.payload) ? repo.payload.default_branch : null);

    // pass in Ref
    let refId = {type: 'branch', id: branch};
    if (query.has('commit')) {
        refId = {
            type: 'commit',
            id: query.get('commit'),
        };
    }

    // comparisons where applicable
    let compareRef = null;
    if (query.has('compareBranch')) {
        compareRef = {
            type: 'branch',
            id: query.get('compareBranch'),
        };
    } else if (query.has('compareCommit')) {
        compareRef = {
            type: 'commit',
            id: query.get('compareCommit'),
        };
    }

    return (

        <div className="mt-5">
            <Breadcrumb>
                <Breadcrumb.Item href={`/repositories`}>Repositories</Breadcrumb.Item>
                <Breadcrumb.Item active href={`/repositories/${repoId}`}>{repoId}</Breadcrumb.Item>
            </Breadcrumb>

            <RepositoryTabs/>

            <Switch>
                <Redirect exact from="/repositories/:repoId" to="/repositories/:repoId/tree"/>
                <Route path="/repositories/:repoId/tree">
                    <TreePage repo={repo.payload} refId={refId} path={query.get('path') || ""}/>
                </Route>
                <Route exact path="/repositories/:repoId/changes">
                    <ChangesPage repo={repo.payload} refId={refId} path={query.get('path') || ""}/>
                </Route>
                <Route exact path="/repositories/:repoId/commits">
                    <CommitsPage repo={repo.payload} refId={refId}/>
                </Route>
                <Route exact path="/repositories/:repoId/branches">
                    <BranchesPage repo={repo.payload}/>
                </Route>
                <Route exact path="/repositories/:repoId/compare">
                    <ComparePage repo={repo.payload} refId={refId} compareRef={compareRef} path={query.get('path') || ""}/>
                </Route>
                <Route exact path="/repositories/:repoId/actions">
                    <ActionsRunsPage repo={repo.payload} refId={refId}/>
                </Route>
                <Route exact path="/repositories/:repoId/actions/:runId">
                    <ActionsRunPage repo={repo.payload} refId={refId}/>
                </Route>
                <Route exact path="/repositories/:repoId/actions/:runId/:hookRunId">
                    <ActionsRunPage repo={repo.payload} refId={refId}/>
                </Route>
                <Route exact path="/repositories/:repoId/settings">
                    <RepoSettingsPage repo={repo.payload}/>
                </Route>
            </Switch>
        </div>
    );
};

export default connect(
    ({ repositories }) => ({ repo: repositories.repo }),
    ({ getRepository })
)(RepositoryExplorerPage);
