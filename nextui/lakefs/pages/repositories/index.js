import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";
import Modal from "react-bootstrap/Modal";

import {RepoIcon, SearchIcon} from "@primer/octicons-react";
import {useEffect, useRef, useState} from "react";
import * as moment from "moment";

import Link from 'next/link';

import Layout from "../../lib/components/layout";
import {DebouncedFormControl} from "../../lib/components/controls";
import {config, repositories} from '../../rest/api';
import {RepositoryCreateForm} from "../../lib/components/repositoryCreateForm";
import {useRouter} from "next/router";
import {mutate} from "swr";


const CreateRepositoryModal = ({show, error, onSubmit, onCancel}) => {

    const { data } = config.useGet();

    return (
        <Modal show={show} onHide={onCancel} size="lg">
            <Modal.Header closeButton>
                <Modal.Title>Create A New Repository</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <RepositoryCreateForm config={data} error={error} onSubmit={onSubmit} onCancel={onCancel}/>
            </Modal.Body>
        </Modal>
    );
};



const RepositoryList = ({ filter, lastListUpdate  }) => {

    const { data , size, setSize, mutate } = repositories.useFilter(filter)

    useEffect(() => {
        mutate();
    }, [lastListUpdate])

    if (!data) return <p>Loading...</p>;

    const currentPage = data[size-1];
    if (!!currentPage && !!currentPage.error) return <Alert variant="danger">{currentPage.error.message}</Alert>

    let paginationButton = (<span/>);
    if (!!currentPage && currentPage.pagination.has_more) {
        paginationButton = (
            <p className="tree-paginator">
                <Button variant="outline-primary" onClick={() => {setSize(size + 1)}}>Load More</Button>
            </p>
        )
    }
    let results = [];
    data.forEach(page => { results = results.concat(...page.results) });

    return (
        <div>
            {results.map(repo => (
                <Row key={repo.id}>
                    <Col className={"mb-2 mt-2"}>
                        <Card>
                            <Card.Body>
                                <h5>
                                    <Link href={`/repositories/${encodeURIComponent(repo.id)}`}>
                                        <a>{repo.id}</a>
                                    </Link>
                                </h5>
                                <p>
                                    <small>
                                        created at <code>{moment.unix(repo.creation_date).toISOString()}</code> ({moment.unix(repo.creation_date).fromNow()})<br/>
                                        default branch: <code>{repo.default_branch}</code>,{' '}
                                        storage namespace: <code>{repo.storage_namespace}</code>
                                    </small>
                                </p>
                            </Card.Body>
                        </Card>
                    </Col>
                </Row>
            ))}
            {paginationButton}
        </div>
    );
};



const Repositories = () => {

    const [filterByPrefix, setFilterByPrefix] = useState('')
    const [showCreateModal, setShowCreateModal] = useState(false)
    const [createError, setCreateError] = useState(null)
    const [lastListUpdate, setLastListUpdate] = useState(new Date())
    const filterField = useRef(null)

    const router = useRouter()

    const createRepo = async (repo) => {
        try {
            await repositories.create(repo)
        } catch (error) {
            setCreateError(error)
            return
        }

        setLastListUpdate(new Date())
        setCreateError(null)
        return await router.push(`/repositories/${encodeURIComponent(repo.name)}`)
    }

    return (
        <Layout>

            <div className="mt-3">

                <div className="action-bar">
                    <Form className="float-left" style={{minWidth: 300}} onSubmit={e => { e.preventDefault(); }}>
                        <Form.Row>
                            <Col>
                                <InputGroup>
                                    <InputGroup.Prepend>
                                        <InputGroup.Text>
                                            <SearchIcon/>
                                        </InputGroup.Text>
                                    </InputGroup.Prepend>
                                    <DebouncedFormControl type="text" placeholder="Find a repository..." autoFocus ref={filterField} onChange={() =>{
                                        setFilterByPrefix(filterField.current.value);
                                    }}/>
                                </InputGroup>
                            </Col>
                        </Form.Row>
                    </Form>
                    <ButtonToolbar className="justify-content-end mb-2">
                        <Button variant="success" onClick={() => {
                            setShowCreateModal(true);
                            setCreateError(null);
                        }}>
                            <RepoIcon/> Create Repository
                        </Button>
                    </ButtonToolbar>
                </div>

                <RepositoryList filter={filterByPrefix} lastListUpdate={lastListUpdate}/>

                <CreateRepositoryModal
                    onCancel={() => setShowCreateModal(false)}
                    show={showCreateModal}
                    error={createError}
                    onSubmit={(repo) => {
                        createRepo(repo);
                    }}/>
            </div>

        </Layout>
    )
}

export default Repositories