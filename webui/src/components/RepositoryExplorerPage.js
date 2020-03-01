import React, {useEffect} from "react";
import {connect} from "react-redux";
import {useParams, useLocation, useHistory, Link} from "react-router-dom";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";

import Octicon, {ChevronDown, GitCommit, File, FileDirectory, GitBranch, Gear, Database, FileBinary}  from "@primer/octicons-react";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";
import {listTree, listBranches} from "../actions/objects";
import {getRepository} from "../actions/repositories";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import * as moment from "moment";

import BranchDropdown from './BranchDropdown';


function useQuery() {
    return new URLSearchParams(useLocation().search);
}

const parentOf = (path) => {
    const parts = path.split(/\//);
    const parentParts = parts.slice(0, parts.length-2);
    const parent =  parentParts.join('/') + '/';
    return (parent === '/') ? "" : parent;
};

const humanSize = (bytes) => {
    if (!bytes) return '0.0 B';
    const e = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes/Math.pow(1024, e)).toFixed(1)+' '+' KMGTP'.charAt(e)+'B';
};

function pathParts(path, rootName = "root") {
    let parts = path.split(/\//);
    let resolved = [{name: rootName, path: ""}];
    if (parts.length === 0) {
        return resolved;
    }

    if (parts[parts.length-1] === "") {
        parts = parts.slice(0, parts.length-1);
    }

    // else
    for (let i=0; i<parts.length; i++) {
        let currentPath = parts.slice(0, i+1).join('/');
        if (currentPath.length > 0) {
            currentPath = `${currentPath}/`;
        }
        resolved.push({
            name: parts[i],
            path: currentPath,
        });
    }

    return resolved;
}

const Na = () => (<span>&mdash;</span>);


let Tree = ({ path, list, repoId, branchId, onNavigate }) => {
    return (
        <div className="tree-container">
            <Card>
                <Card.Header>
                    <PartsNavigator path={path} repoId={repoId} onNavigate={onNavigate}/>
                </Card.Header>
                <Card.Body>
                    {(!!list.error) ?
                        <Alert variant="danger" className="tree-error">{list.error}</Alert> :
                        <Table borderless hover size="sm">
                            <tbody>
                            {list.payload.results.map(entry => (
                                <tr key={entry.path}>
                                    <td className="tree-path">
                                        <Octicon icon={entry.path_type === 'TREE' ? FileDirectory : File}/> {' '}
                                        <Button variant="link" onClick={() => { onNavigate(entry.path) }}>
                                            {(path.length > 0) ? entry.path.substr(path.length) : entry.path}
                                        </Button>
                                    </td>
                                    <td className="tree-size">
                                    {(entry.path_type === 'OBJECT') ?
                                        <span>{humanSize(entry.size_bytes)}</span> :
                                        <Na/>
                                    }
                                    </td>
                                    <td className="tree-modified">
                                        {(entry.path_type === 'OBJECT') ?
                                            <span>{moment.unix(entry.mtime).fromNow()}</span> :
                                            <Na/>
                                        }
                                    </td>

                                </tr>
                            ))}
                            </tbody>
                        </Table>
                    }
                </Card.Body>
            </Card>
        </div>
    );
};

const PartsNavigator = ({ path, onNavigate }) => {
    const parts = pathParts(path);
    return (
        <span>
            Path: {' '}
            {parts.map((part, i) => (
                <span key={i}>
                    <Button variant="link" key={part} onClick={() => { onNavigate(part.path); }}>{part.name}</Button>
                    <span> <strong>{'/'}</strong> </span>
                </span>
            ))}
        </span>
    );
};

let TreePage = ({repo, repoId, branchId, path, list, listTree, getRepository}) => {

    const history = useHistory();
    const location = useLocation();

    useEffect(() => {
        getRepository(repoId);
    }, [repoId, getRepository]);

    // we have a repo object
    const branch = (!!branchId) ? branchId : ((!!repo.payload) ? repo.payload.default_branch : null);

    useEffect(() => {
        if (branch != null)
            listTree(repoId, branch, path);
    },[repoId, branch, path, listTree]);

    if (!!!repo.payload || repo.loading || list.loading) {
        return <p>Loading...</p>;
    }

    return (
        <div className="mt-3">
            <div className="action-bar">
                <ButtonToolbar className="float-left mb-2">
                    <BranchDropdown repoId={repoId} selectedBranch={branch} selectRef={(ref) => {
                        const params = new URLSearchParams(location.search);
                        params.set('branch', ref.id);
                        history.push({...location, search: params.toString()})
                    }}/>

                    <Button variant="light">
                        Compared to: <strong>{branch}</strong> <Octicon icon={ChevronDown}/>
                    </Button>
                </ButtonToolbar>
                <ButtonToolbar className="float-right mb-2">
                    <Button variant="light">
                        Upload File
                    </Button>
                    <Button variant="success">
                        <Octicon icon={GitCommit}/> Commit Changes
                    </Button>
                </ButtonToolbar>
            </div>

            <Tree
                repoId={repoId}
                branch={branch}
                onNavigate={(path) => {
                    const params = new URLSearchParams(location.search);
                    params.set('path', path);
                    history.push({...location, search: params.toString()});
                }}
                list={list}
                path={path}/>

        </div>
    );
};

TreePage = connect(
    ({ objects, repositories }) => ({ list: objects.list, repo: repositories.repo }),
    ({ listTree, getRepository })
)(TreePage);


export const RepositoryExplorerPage = ({ currentTab, onNavigate }) => {

    const { repoId } = useParams();
    const query = useQuery();

    return (
        <Tabs
            activeKey={currentTab}
            onSelect={onNavigate}
            className={"mt-5"}>
            <Tab eventKey={`/repositories/${repoId}/tree`} title={<span><Octicon icon={Database}/>  Objects</span>}>
                <TreePage repoId={repoId} branchId={query.get('branch')} path={query.get('path') || ""}/>
            </Tab>
            <Tab eventKey={`/repositories/${repoId}/commits`} title={<span><Octicon icon={GitCommit}/>  Commits</span>}>

            </Tab>
            <Tab eventKey={`/repositories/${repoId}/branches`} title={<span><Octicon icon={GitBranch}/>  Branches</span>}>

            </Tab>
            <Tab eventKey={`/repositories/${repoId}/settings`} title={<span><Octicon icon={Gear}/>  Settings</span>}>

            </Tab>
        </Tabs>
    );
};