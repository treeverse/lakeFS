import React, {useEffect} from "react";
import {useHistory, useLocation} from "react-router-dom";
import {connect} from "react-redux";

import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";

import Octicon, {GitCommit} from "@primer/octicons-react";

import {listTree} from "../actions/objects";
import {getRepository} from "../actions/repositories";
import {diff} from "../actions/refs";
import BranchDropdown from "./BranchDropdown";
import Tree from "./Tree";


const TreePage = ({repoId, refId, compareRef, path, list, listTree, diff, diffResults}) => {

    const history = useHistory();
    const location = useLocation();

    let compare;
    if (!!compareRef) {
        compare = compareRef;
    } else if (refId.type === 'branch') {
        compare = {...refId, description: 'committed changes'};
    } else {
        // no comparison to be made
    }

    const compareId = (!!compare) ? compare.id : "";

    useEffect(() => {
        listTree(repoId, refId.id, path);
    }, [repoId, refId.id, path, listTree]);

    useEffect(() => {
        if (!!compare) diff(repoId, refId.id, compare.id);
        // (compareId is computed from compare which is not included in the deps list)
        // eslint-disable-next-line
    },[repoId, refId.id, listTree, diff, compareId]);

    return (
        <div className="mt-3">
            <div className="action-bar">
                <ButtonToolbar className="float-left mb-2">
                    <BranchDropdown repoId={repoId} selected={refId} selectRef={(ref) => {
                        const params = new URLSearchParams(location.search);
                        params.set('branch', ref.id);
                        params.delete('commit'); // if we explicitly selected a branch, remove an existing commit if any
                        history.push({...location, search: params.toString()})
                    }}/>
                    {(!!compare && !!compare.id) ?
                        (
                            <Button variant="link" disabled>
                            Compared to: <strong>{compare.id}</strong> {(!!compare.description) ? (<small>({compare.description})</small>) : (<span/>)}
                            </Button>
                        ): (<span/>)
                    }
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
                refId={refId}
                onNavigate={(path) => {
                    const params = new URLSearchParams(location.search);
                    params.set('path', path);
                    history.push({...location, search: params.toString()});
                }}
                diffResults={diffResults}
                list={list}
                path={path}/>
        </div>
    );
};

export default connect(
    ({ objects, repositories, refs }) => ({ list: objects.list, repo: repositories.repo, diffResults: refs.diff }),
    ({ listTree, getRepository, diff })
)(TreePage);