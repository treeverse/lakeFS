import React from 'react';

import {connect} from "react-redux";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";

import {Link, useParams} from "react-router-dom";
import {useHistory} from "react-router-dom";

import 'react-bootstrap-typeahead/css/Typeahead.css';
import 'react-bootstrap-typeahead/css/Typeahead-bs4.css';



import {createBranch, selectSourceBranch, searchBranch} from "../actions/branches";
import BranchAutoCompleter from './BranchAutoCompleter';


const BranchCreate = ({createError, createBranch, searchBranch, selectSourceBranch, sourceBranchSelection, prefixList }) => {

    const history = useHistory();
    let { repoId } = useParams();

    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card>
                    <Card.Header>Create Branch</Card.Header>
                    <Card.Body>
                        <Form className={"mt-5"} onSubmit={(e) => {
                            e.preventDefault();
                            createBranch(
                                repoId,
                                e.target.id.value,
                                sourceBranchSelection,
                                () => { history.replace(`/repositories/${repoId}/branches`); }
                            );
                        }}>
                            <Form.Group as={Row} controlId="id">
                                <Form.Label column sm="2">Branch ID</Form.Label>
                                <Col sm={6}>
                                    <Form.Control type="text" autoFocus/>
                                </Col>
                            </Form.Group>

                            <Form.Group as={Row} controlId="sourceBranch">
                                <Form.Label column sm="2">From Branch</Form.Label>
                                <Col sm={6}>
                                    <BranchAutoCompleter
                                        id="sourceBranch"
                                        onChange={selectSourceBranch}
                                        branchList={prefixList}
                                        searchBranch={query => {
                                            searchBranch(repoId, query);
                                        }}/>
                                </Col>
                            </Form.Group>

                            {!!createError ?
                                <Row>
                                    <Col md={{span: 6, offset: 2}} >
                                        <Alert variant={"danger"}>{createError}</Alert>
                                    </Col>
                                </Row>: <span/>}

                            <Row>
                                <Col md={{span: 6, offset: 2}} >
                                    <Button variant="success" type="submit" className="mr-2">Create Branch</Button>
                                    <Button variant="secondary" as={Link} to={`/repositories/${repoId}`}>Cancel</Button>
                                </Col>
                            </Row>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
};

export default connect(
    ({ branches }) => ({
        createError: branches.createError,
        sourceBranchSelection: branches.sourceBranchSelection,
        prefixList: branches.prefixList,
    }),
    {createBranch, selectSourceBranch, searchBranch},
)(BranchCreate);