import React, {useEffect} from "react";
import {useLocation, useHistory} from "react-router-dom";

import BranchDropdown from "./BranchDropdown";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Alert from "react-bootstrap/Alert";

import {connect} from "react-redux";
import {logCommits} from '../actions/commits';
import * as moment from "moment";
import ListGroup from "react-bootstrap/ListGroup";
import ListGroupItem from "react-bootstrap/ListGroupItem";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import Octicon, {Code, Clippy} from "@primer/octicons-react";


const CommitWidget = ({commit}) => {
    return (
        <ListGroupItem>
            <div className="clearfix">
                <div className="float-left">
                    <h6>{commit.message}</h6>
                    <p>
                        <small>
                            <strong>{commit.committer}</strong> committed at <strong>{moment.unix(commit.creation_date).format("MM/DD/YYYY HH:mm:ss")}</strong> ({moment.unix(commit.creation_date).fromNow()})
                        </small>
                    </p>
                </div>
                <div className="float-right">
                    <ButtonGroup className="commit-actions">
                        <Button variant="light">
                            <Octicon icon={Clippy}/>
                        </Button>
                        <Button variant="light">
                            <Octicon icon={Code}/>
                        </Button>
                        <Button variant="light">
                            {(commit.id.length > 16) ? commit.id.substr(0, 8) : commit.id}
                        </Button>
                    </ButtonGroup>
                </div>
            </div>
        </ListGroupItem>
    );
};


const CommitsPage = ({repoId, branchId, logCommits, log }) => {

    const history = useHistory();
    const location = useLocation();

    useEffect(() => {
        logCommits(repoId, branchId);
    },[logCommits, repoId, branchId]);

    let body;
    if (log.loading) {
        body = (<Alert variant="info">Loading</Alert>);
    } else if (!!log.error) {
        body = (<Alert variant="danger">{log.error}</Alert> );
    } else {
        body = (
            <ListGroup>
                {log.payload.map(commit => (
                    <CommitWidget key={commit.id} commit={commit}/>
                ))}
            </ListGroup>
        );
    }

    return (
        <div className="mt-3 mb-5">
            <div className="action-bar">
                <ButtonToolbar className="float-left mb-2">
                    <BranchDropdown repoId={repoId} selectedBranch={branchId} withCommits={false} selectRef={(ref) => {
                        const params = new URLSearchParams(location.search);
                        params.set('branch', ref.id);
                        history.push({...location, search: params.toString()})
                    }}/>
                </ButtonToolbar>
            </div>
            {body}
        </div>
    );
};

export default connect(
    ({ repositories, commits }) => ({ repo: repositories.repo, log: commits.log }),
    ({ logCommits })
)(CommitsPage);