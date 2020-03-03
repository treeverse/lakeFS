import React, {useEffect} from "react";
import {useHistory, useLocation} from "react-router-dom";
import {connect} from "react-redux";

import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";

import Octicon, {ChevronDown, GitCommit} from "@primer/octicons-react";

import {listTree} from "../actions/objects";
import {getRepository} from "../actions/repositories";
import BranchDropdown from "./BranchDropdown";
import Tree from "./Tree";


const TreePage = ({repoId, branchId, path, list, listTree}) => {

    const history = useHistory();
    const location = useLocation();

    useEffect(() => {
        listTree(repoId, branchId, path);
    },[repoId, branchId, path, listTree]);

    return (
        <div className="mt-3">
            <div className="action-bar">
                <ButtonToolbar className="float-left mb-2">
                    <BranchDropdown repoId={repoId} selectedBranch={branchId} selectRef={(ref) => {
                        const params = new URLSearchParams(location.search);
                        params.set('branch', ref.id);
                        history.push({...location, search: params.toString()})
                    }}/>

                    <Button variant="light">
                        Compared to: <strong>{branchId}</strong> <Octicon icon={ChevronDown}/>
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
                branchId={branchId}
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

export default connect(
    ({ objects, repositories }) => ({ list: objects.list, repo: repositories.repo }),
    ({ listTree, getRepository })
)(TreePage);