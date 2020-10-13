import React from "react";
import {connect} from "react-redux";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import {Container} from "react-bootstrap";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

const RepoSettingsPage = ({repo}) => {
    const body = (
        <>
            <div className="section-title"><h4>General</h4></div>
            <Container>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">Repository name</Form.Label>
                    <Col md={{span:4}}><Form.Control readOnly value={repo.id} type="text"/></Col>
                </Row>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">Storage namespace</Form.Label>
                    <Col md={{span:4}}><Form.Control readOnly value={repo.storage_namespace} type="text"/></Col>
                </Row>
                <Row>
                    <Form.Label column md={{span:2}} className="mb-3">Default branch</Form.Label>
                    <Col md={{span:4}}><Form.Control readOnly value={repo.default_branch} type="text"/></Col>
                </Row>
            </Container>
        </>
    );
    return (
        <div className="mt-3 mb-5">
            <div className="action-bar">
                <h3>Settings</h3>
            </div>

            {body}
        </div>
    );
};

export default connect()(RepoSettingsPage);
