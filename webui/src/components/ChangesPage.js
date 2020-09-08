import React, {useEffect} from "react";
import {useLocation, useHistory, Link} from "react-router-dom";

import RefDropdown from "./RefDropdown";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Alert from "react-bootstrap/Alert";

import {connect} from "react-redux";
import {logCommits, logCommitsPaginate} from '../actions/commits';
import * as moment from "moment";
import ListGroup from "react-bootstrap/ListGroup";
import ListGroupItem from "react-bootstrap/ListGroupItem";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import {LinkIcon, LinkExternalIcon, DiffIcon} from "@primer/octicons-react";
import ClipboardButton from "./ClipboardButton";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Table from "react-bootstrap/Table";


const ChangesPage = ({repo, refId, logCommits,logCommitsPaginate, log }) => {

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
            <>
            <ListGroup className="commit-list pagination-group">
                {log.payload.results.map((commit, i) => (
                    <CommitWidget key={commit.id} commit={commit} repo={repo} previous={(i < log.payload.results.length-1) ? log.payload[i+1] : null}/>
                ))}
            </ListGroup>
                {(log.payload.pagination.has_more) ? (
                    <p className="tree-paginator">
                        <Button variant="outline-primary" onClick={() => {
                            logCommitsPaginate(repo.id, refId.id,log.payload.pagination.next_offset)
                        }}>Load More</Button>
                    </p>
                ) : (<span/>)}
            </>
        );
    }

    return (
        <div className="mt-3 mb-5">
            <div className="action-bar">
                <ButtonToolbar className="float-left mb-2">
                    <RefDropdown repo={repo} selected={refId} withCommits={false} selectRef={(ref) => {
                        const params = new URLSearchParams(location.search);
                        if (ref.type === 'branch') {
                            params.set('branch', ref.id);
                            params.delete('commit'); // if we explicitly selected a branch, remove an existing commit if any
                        } else {
                            params.set('commit', ref.id);
                            params.delete('branch'); // if we explicitly selected a commit, remove an existing branch if any
                        }
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
    ({ logCommits, logCommitsPaginate })
)(ChangesPage);
