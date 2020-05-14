import Modal from "react-bootstrap/Modal";
import {RepositoryCreateForm} from "./RepositoryCreateForm";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import {Link} from "react-router-dom";
import * as moment from "moment";
import {connect} from "react-redux";
import {createRepository, createRepositoryDone, filterRepositories, listRepositories, listRepositoriesPaginate} from "../actions/repositories";
import React, {useEffect, useRef, useState} from "react";
import Form from "react-bootstrap/Form";
import InputGroup from "react-bootstrap/InputGroup";
import Octicon, {Repo, Search} from "@primer/octicons-react";
import {DebouncedFormControl} from "./DebouncedInput";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";

const CreateRepositoryModal = ({show, error, onSubmit, onCancel}) => {
    return (
        <Modal show={show} onHide={onCancel} size="lg">
            <Modal.Header closeButton>
                <Modal.Title>Create A New Repository</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <RepositoryCreateForm error={error} onSubmit={onSubmit} onCancel={onCancel}/>
            </Modal.Body>
        </Modal>
    );
};

const RepositoryList = ({ list, paginate }) => {

    if (list.loading || !list.payload) {
        return <p>Loading...</p>;
    }

    if (!!list.error) {
        return (<Alert variant="danger">{list.error}</Alert>);
    }

    let paginator = (<span/>);
    if (list.payload.pagination.has_more) {
        paginator = (
            <p className="tree-paginator">
                <Button variant="outline-primary" onClick={() => {paginate(list.payload.pagination.next_offset)}}>Load More</Button>
            </p>
        )
    }

    return (
        <div>
            {list.payload.results.map(repo => (
                <Row key={repo.id}>
                    <Col className={"mb-2 mt-2"}>
                        <Card>
                            <Card.Body>
                                <h5><Link to={`/repositories/${repo.id}/tree`}>{repo.id}</Link></h5>
                                <p>
                                    <small>
                                        created at <code>{moment.unix(repo.creation_date).toISOString()}</code> ({moment.unix(repo.creation_date).fromNow()})<br/>
                                        default branch: <code>{repo.default_branch}</code>,{' '}
                                        storage namesapce: <code>{repo.bucket_name}</code>
                                    </small>
                                </p>
                            </Card.Body>
                        </Card>
                    </Col>
                </Row>
            ))}
            {paginator}
        </div>
    );
};

export const RepositoryListPage = connect(
    ({ repositories }) => {
        const {list, create, createIndex} = repositories;
        return {list, create, createIndex};
    },
    ({ listRepositories, listRepositoriesPaginate,  filterRepositories, createRepository, createRepositoryDone })
)(({listRepositories, listRepositoriesPaginate, filterRepositories, createRepository, createRepositoryDone, list, create, createIndex }) => {

    const [showingCreateModal, setShowCreateModal] = useState(false);
    const closeCreateModal = () => setShowCreateModal(false);
    const showCreateModal = () => setShowCreateModal(true);

    const filterField = useRef(null);

    useEffect(()=> {
        listRepositories();
        if (create.done) {
            setShowCreateModal(false);
        }
    }, [listRepositories, create.done]);

    return (
        <div className="mt-3">
            <div className="action-bar">
                <Form className="float-left" style={{minWidth: 300}} onSubmit={e => { e.preventDefault(); }}>
                    <Form.Row>
                        <Col>
                            <InputGroup>
                                <InputGroup.Prepend>
                                    <InputGroup.Text>
                                        <Octicon icon={Search}/>
                                    </InputGroup.Text>
                                </InputGroup.Prepend>
                                <DebouncedFormControl type="text" placeholder="Find a repository..." autoFocus ref={filterField} onChange={() =>{
                                    filterRepositories(filterField.current.value, 1000);
                                }}/>
                            </InputGroup>
                        </Col>
                    </Form.Row>
                </Form>
                <ButtonToolbar className="justify-content-end mb-2">
                    <Button variant="success" onClick={() => {
                        createRepositoryDone();
                        showCreateModal();
                    }}>
                        <Octicon icon={Repo}/> Create Repository
                    </Button>
                </ButtonToolbar>
            </div>
            <RepositoryList list={list} paginate={listRepositoriesPaginate}/>
            <CreateRepositoryModal
                onCancel={closeCreateModal}
                show={showingCreateModal}
                error={create.error}
                onSubmit={(repo) => createRepository(repo)}/>
        </div>
    );
});
