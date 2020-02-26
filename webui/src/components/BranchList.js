import React, {useEffect, useRef, useState} from "react";

import {connect} from "react-redux";

import {useParams, Link, useHistory} from "react-router-dom";
import Card from "react-bootstrap/Card";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";
import Octicon, {Trashcan, Sync, Clippy} from "@primer/octicons-react";
import ListGroup from "react-bootstrap/ListGroup";
import Tooltip from "react-bootstrap/Tooltip";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Form from "react-bootstrap/Form";
import Col from "react-bootstrap/Col";

import ConfirmationModal from "./ConfirmationModal";
import {listBranches} from '../actions/branches'
import {deleteRepository} from "../actions/repositories";
import Alert from "react-bootstrap/Alert";



const BranchList = ({ branches, listBranches, deleteRepository, error }) => {

    let history = useHistory();

    let { repoId } = useParams();

    useEffect(() => {
        listBranches(repoId, (!!searchFilter) ? searchFilter.value : "");
    }, [repoId, listBranches]);

    const searchFilter = useRef(null);

    const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);

    let content = (
        <h6 className="text-center">Loading...</h6>
    );

    if (!branches.loading) {
        content = (
            <ListGroup className={"repo-list float-none"}>
                { branches.list.map(branch => (
                    <ListGroup.Item key={branch.id}>
                        <Link to={`/repositories/${repoId}/branch/${branch.id}`}>{branch.id}</Link>
                        <small className="description">
                            {branch.commit_id}
                            {' '}
                            <OverlayTrigger placement="top" overlay={<Tooltip>Copy Commit Hash to Clipboard</Tooltip>}>
                                <span className={"action"} onClick={(e) => {
                                    e.preventDefault();
                                    // copy to clipboard
                                    const textarea = document.createElement('textarea');
                                    document.body.appendChild(textarea);
                                    textarea.value = branch.commit_id;
                                    textarea.select();
                                    document.execCommand('copy');
                                    textarea.remove()}}>
                                        <Octicon icon={Clippy}/>
                                </span>
                            </OverlayTrigger>
                        </small>
                    </ListGroup.Item>
                ))}
            </ListGroup>
        );
    }

    return (
        <Card>
            <Card.Header>
                <Link to={`/repositories`}>Repositories</Link> / <Link to={`/repositories/${repoId}`}>{repoId}</Link>
            </Card.Header>
            <Card.Body>

                <Form className="float-left mb-2" style={{minWidth: 300}} onSubmit={e => { e.preventDefault(); }}>
                    <Form.Row>
                        <Col>
                            <Form.Control type="text" placeholder="filter branches..." autoFocus ref={searchFilter} onChange={() => {
                                listBranches(repoId, (!!searchFilter) ? searchFilter.current.value : "", false);
                            }}/>
                        </Col>
                    </Form.Row>
                </Form>

                <ButtonToolbar className="justify-content-end mb-2" aria-label="Toolbar with Button groups">

                    <Button variant="outline-success" className="ml-2" as={Link} to={`/repositories/${repoId}/branches/create`}>
                        Create Branch
                    </Button>

                    <OverlayTrigger placement="bottom" overlay={<Tooltip id="tooltip-refresh-repo-branches">Refresh Branch List</Tooltip>}>
                        <Button variant="outline-primary" className="ml-2" onClick={(e) => {
                            e.preventDefault();
                            listBranches(repoId, (!!searchFilter) ? searchFilter.current.value : "");
                        }}>
                            <Octicon icon={Sync}/>
                        </Button>
                    </OverlayTrigger>


                    <OverlayTrigger placement="bottom" overlay={<Tooltip id="tooltip-delete-repo">Delete Repository</Tooltip>}>
                        <Button variant="outline-danger" className="ml-2" onClick={() => { setShowDeleteConfirm(true); }}>
                            <Octicon icon={Trashcan}/>
                        </Button>
                    </OverlayTrigger>

                </ButtonToolbar>
                <div className="mt-3">
                    { (!!error) ? <p>
                        <Alert variant="danger">Error deleting repository: {error}</Alert>
                    </p> : <span/>}

                    {content}
                </div>
            </Card.Body>

            <ConfirmationModal
                msg={
                    <div>
                    <p>
                        Are you sure you'd like to delete repository <strong>{`${repoId}`}</strong>?
                    </p>
                    <p>
                        <em className="danger-text"><strong>Caution:</strong> This action is not reversible.<br/>Data will remain in the underlying storage but will not be unaddressable.</em>
                    </p>
                    </div>
                }
                show={showDeleteConfirm}
                onConfirm={() => {
                    setShowDeleteConfirm(false);
                    deleteRepository(repoId, () => {
                        history.push('/repositories');
                    });
                }}
                onHide={() => {setShowDeleteConfirm(false); }}/>
        </Card>
    );
};


export default connect(
    ({ branches, repositories }) => ({ branches, error: repositories.deleteError }),
    {listBranches, deleteRepository}
)(BranchList);