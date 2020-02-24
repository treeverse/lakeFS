import Table from "react-bootstrap/Table";
import React from "react";

import {connect} from "react-redux";

import {Link} from "react-router-dom";
import Button from "react-bootstrap/Button";
import ListGroup from "react-bootstrap/ListGroup";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Card from "react-bootstrap/Card";

import './RepositoryList.css';





const repoList = ({ repositories }) => {
    return (
        <Card>
            <Card.Header>
                <h5>Repositories</h5>
            </Card.Header>
            <Card.Body>
                <ButtonToolbar className="justify-content-end mb-3" aria-label="Toolbar with Button groups">
                    <Button>Create Repository</Button>
                </ButtonToolbar>
                <ListGroup className={"repo-list float-none"}>
                    { repositories.list.map(repo => {
                        return (
                            <ListGroup.Item>
                                <Link to={`/repositories/${repo.name}`}>{repo.name}</Link>
                            </ListGroup.Item>
                        );
                    })}
                </ListGroup>
            </Card.Body>
        </Card>
    );
};

export default connect(({ repositories }) => ({repositories}), null)(repoList);