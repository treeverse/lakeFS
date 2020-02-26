import React, {useEffect} from 'react';

import {connect} from "react-redux";
import Card from "react-bootstrap/Card";
import {Link, useParams} from "react-router-dom";

import {FileExplorer} from "./FileExplorer";
import {navigate, objectsSetBranch} from "../actions/objects";

const BranchExplorer = ({objects, navigate, objectsSetBranch}) => {

    const { repoId, branchId, loading } = useParams();

    useEffect(()=> {
        objectsSetBranch(repoId, branchId);
        navigate(repoId, branchId, objects.path);
        return function cleanup() {
            objectsSetBranch("", ""); // reset state on unmount
        };
    }, [repoId, branchId, navigate, objectsSetBranch, objects.path]);

    const {path, entries} = objects;

    return (
        <Card>
            <Card.Header>
                <Link to={`/repositories`}>Repositories</Link> / <Link to={`/repositories/${repoId}`}>{repoId}</Link> / <Link to={`/repositories/${repoId}/branch/${branchId}`}>{branchId}</Link>
            </Card.Header>
            <Card.Body>
                <p>{path}</p>
                <FileExplorer
                    repoId={repoId}
                    branchId={branchId}
                    path={path}
                    entries={entries}
                    loading={loading}
                    onNavigate={(nav) => {
                        navigate(repoId, branchId, nav);
                    }}/>
            </Card.Body>
        </Card>
    );
};

export default connect(({ objects }) => ({ objects }), {navigate, objectsSetBranch})(BranchExplorer);