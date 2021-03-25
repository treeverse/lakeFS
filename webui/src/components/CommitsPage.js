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
import {LinkIcon, LinkExternalIcon, DiffIcon, PlayIcon} from "@primer/octicons-react";
import ClipboardButton from "./ClipboardButton";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Table from "react-bootstrap/Table";


const CommitWidget = ({repo, commit, previous}) => {

    const buttonVariant = "secondary";

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
                    {(!!commit.metadata) ? (
                        <Table className="commit-metadata" size="sm" hover>
                            <thead>
                                <tr>
                                    <th>Metadata Key</th>
                                    <th>Value</th>
                                </tr>
                            </thead>
                            <tbody>
                            {Object.getOwnPropertyNames(commit.metadata).map((key, i) => {
                                return (
                                    <tr key={`commit-${commit.id}-metadata-${i}`}>
                                        <td><code>{key}</code></td>
                                        <td><code>{commit.metadata[key]}</code></td>
                                    </tr>
                                );
                            })}
                            </tbody>
                        </Table>
                    ) : (<span/>)}
                </div>
                <div className="float-right">
                    <ButtonGroup className="commit-actions">
                        <ClipboardButton variant={buttonVariant} text={`s3://${repo.id}/${commit.id}`} tooltip="copy S3 URI to clipboard" icon={<LinkExternalIcon/>}/>
                        <ClipboardButton variant={buttonVariant} text={`lakefs://${repo.id}@${commit.id}`} tooltip="copy URI to clipboard" icon={<LinkIcon/>}/>
                        <ClipboardButton variant={buttonVariant} text={commit.id} tooltip="copy ID to clipboard"/>
                        <OverlayTrigger placement="bottom" overlay={<Tooltip>View commit runs</Tooltip>}>
                            <Button variant={buttonVariant} as={Link} to={`/repositories/${repo.id}/actions?commit=${commit.id}`} >
                                <PlayIcon/>
                            </Button>
                        </OverlayTrigger>
                        <OverlayTrigger placement="bottom" overlay={<Tooltip>Explore objects at commit</Tooltip>}>
                            <Button variant={buttonVariant} as={Link} to={`/repositories/${repo.id}/tree?commit=${commit.id}`}>
                                {commit.id.substr(0, 16)}
                            </Button>
                        </OverlayTrigger>
                        {(!!previous && !!previous.parents) ? (
                            <OverlayTrigger placement="bottom" overlay={<Tooltip>Diff with previous commit</Tooltip>}>
                                <Button variant={buttonVariant} as={Link} to={`/repositories/${repo.id}/tree?commit=${commit.id}&compareCommit=${previous.id}`}>
                                    <DiffIcon/>
                                </Button>
                            </OverlayTrigger>
                        ) : (<span/>)}

                    </ButtonGroup>
                </div>
            </div>
        </ListGroupItem>
    );
};


const CommitsPage = ({repo, refId, logCommits,logCommitsPaginate, log }) => {

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
)(CommitsPage);
