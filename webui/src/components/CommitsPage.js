import React, {useEffect} from "react";
import {useLocation, useHistory, Link} from "react-router-dom";

import RefDropdown from "./RefDropdown";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Alert from "react-bootstrap/Alert";

import {connect} from "react-redux";
import {logCommits} from '../actions/commits';
import * as moment from "moment";
import ListGroup from "react-bootstrap/ListGroup";
import ListGroupItem from "react-bootstrap/ListGroupItem";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import Octicon, {Code, Link as LinkIcon} from "@primer/octicons-react";
import ClipboardButton from "./ClipboardButton";


const CommitWidget = ({repo, commit, previous}) => {

    let prevQuery = '';
    if (!!previous) {
        prevQuery = `&compareCommit=${previous.id.slice(0, 16)}`;
    }

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
                        <ClipboardButton variant="light" text={`lakefs://${repo.id}@${commit.id}`} tooltip="copy URI to clipboard" icon={LinkIcon}/>
                        <ClipboardButton variant="light" text={commit.id} tooltip="copy ID to clipboard"/>
                        <Button variant="light" as={Link} to={`/repositories/${repo.id}/tree?commit=${commit.id.slice(0, 16)}${prevQuery}`}>
                            <Octicon icon={Code}/>
                        </Button>
                        <Button variant="light" as={Link} to={`/repositories/${repo.id}/tree?commit=${commit.id.slice(0, 16)}${prevQuery}`}>
                            {(commit.id.length > 16) ? commit.id.substr(0, 8) : commit.id}
                        </Button>
                    </ButtonGroup>
                </div>
            </div>
        </ListGroupItem>
    );
};


const CommitsPage = ({repo, refId, logCommits, log }) => {

    const history = useHistory();
    const location = useLocation();

    useEffect(() => {
        logCommits(repo.id, refId.id);
    },[logCommits, repo.id, refId.id]);

    let body;
    if (log.loading) {
        body = (<Alert variant="info">Loading</Alert>);
    } else if (!!log.error) {
        body = (<Alert variant="danger">{log.error}</Alert> );
    } else {
        body = (
            <ListGroup>
                {log.payload.filter(commit => !!commit.parents).map((commit, i) => (
                    <CommitWidget key={commit.id} commit={commit} repo={repo} previous={(i < log.payload.length-1) ? log.payload[i+1] : null}/>
                ))}
            </ListGroup>
        );
    }

    return (
        <div className="mt-3 mb-5">
            <div className="action-bar">
                <ButtonToolbar className="float-left mb-2">
                    <RefDropdown repo={repo} selected={refId} withCommits={false} selectRef={(ref) => {
                        const params = new URLSearchParams(location.search);
                        params.set('branch', ref.id);
                        params.delete('commit'); // if we explicitly selected a branch, remove an existing commit if any
                        history.push({...location, search: params.toString()})
                    }}/>
                </ButtonToolbar>
            </div>
            {body}
        </div>
    );
};

export default connect(
    ({ commits }) => ({ log: commits.log }),
    ({ logCommits })
)(CommitsPage);