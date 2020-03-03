import React, {useEffect} from "react";
import {useParams, useLocation} from "react-router-dom";

import Breadcrumb from "react-bootstrap/Breadcrumb";
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";
import Octicon, {GitCommit, GitBranch, Gear, Database}  from "@primer/octicons-react";

import TreePage from './TreePage';
import CommitsPage from './CommitsPage';
import {connect} from "react-redux";
import {getRepository} from "../actions/repositories";


function useQuery() {
    return new URLSearchParams(useLocation().search);
}

const RepositoryExplorerPage = ({ repo, getRepository, currentTab, onNavigate }) => {

    const { repoId } = useParams();
    const query = useQuery();

    useEffect(() => {
        getRepository(repoId);
    }, [getRepository, repoId]);


    if (repo.loading) {
        return <p>Loading...</p>;
    }

    // we have a repo object
    const branchId = query.get('branch');
    const branch = (!!branchId) ? branchId : ((!!repo.payload) ? repo.payload.default_branch : null);

    return (
        <div className="mt-5">
            <Breadcrumb>
                <Breadcrumb.Item href={`/`}>Repositories</Breadcrumb.Item>
                <Breadcrumb.Item active href={`/repositories/${repoId}`}>{repoId}</Breadcrumb.Item>
            </Breadcrumb>
            <Tabs activeKey={currentTab} onSelect={onNavigate} className={"mt-5"}>
                <Tab eventKey={`/repositories/${repoId}/tree`} title={<span><Octicon icon={Database}/>  Objects</span>}>
                    <TreePage repoId={repoId} branchId={branch} path={query.get('path') || ""}/>
                </Tab>
                <Tab eventKey={`/repositories/${repoId}/commits`} title={<span><Octicon icon={GitCommit}/>  Commits</span>}>
                    <CommitsPage repoId={repoId} branchId={branch}/>
                </Tab>
                <Tab eventKey={`/repositories/${repoId}/branches`} title={<span><Octicon icon={GitBranch}/>  Branches</span>}>

                </Tab>
                <Tab eventKey={`/repositories/${repoId}/settings`} title={<span><Octicon icon={Gear}/>  Settings</span>}>

                </Tab>
            </Tabs>
        </div>
    );
};

export default connect(
    ({ repositories }) => ({ repo: repositories.repo }),
    ({  getRepository })
)(RepositoryExplorerPage);