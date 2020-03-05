import React from 'react';

import {connect} from "react-redux";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {Link} from "react-router-dom";
import {useHistory} from "react-router-dom";


import {createRepository} from "../actions/repositories";
import Card from "react-bootstrap/Card";

const RepositoryCreate = ({ createError, createRepository }) => {

    const history = useHistory();

    return (
        <Row>
            <Col md={{offset: 2, span: 8}}>
                <Card>
                    <Card.Header>Create Repository</Card.Header>
                    <Card.Body>
                        <Form className={"mt-5"} onSubmit={(e) => {
                            e.preventDefault();
                            createRepository(
                                e.target.id.value,
                                e.target.bucketName.value,
                                e.target.defaultBranch.value,
                                () => {
                                    history.replace('/repositories');
                                });
                        }}>
                            <Form.Group as={Row} controlId="id">
                                <Form.Label column sm="2">Repository ID</Form.Label>
                                <Col sm={6}>
                                    <Form.Control type="text" autoFocus/>
                                </Col>
                            </Form.Group>

                            <Form.Group as={Row} controlId="bucketName">
                                <Form.Label column sm="2">Storage Namespace</Form.Label>
                                    <Col sm={6}>
                                        <Form.Control type="text" placeholder="i.e. S3 Bucket name"/>
                                    </Col>
                            </Form.Group>
                            <Form.Group as={Row} controlId="defaultBranch">
                                <Form.Label column sm="2">Default Branch</Form.Label>
                                <Col sm={6}>
                                    <Form.Control type="text" placeholder="defaultBranch" defaultValue={"master"}/>
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
                                    <Button variant="success" type="submit" className="mr-2">Create Repository</Button>
                                    <Button variant="secondary" as={Link} to={"/repositories"}>Cancel</Button>
                                </Col>
                            </Row>
                        </Form>
                    </Card.Body>
                </Card>
            </Col>
        </Row>
    );
}

export default connect(
    ({ repositories}) => ({ createError: repositories.createError }),
    {createRepository}
)(RepositoryCreate);