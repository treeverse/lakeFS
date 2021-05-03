import React from "react";

import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";

import {RepoIcon, SearchIcon} from "@primer/octicons-react";
import {useState} from "react";
import moment from "moment";

import Layout from "../../lib/components/layout";
import {
    ActionsBar,
    Loading,
    useDebouncedState
} from "../../lib/components/controls";
import {config, repositories} from '../../lib/api';
import {RepositoryCreateForm} from "../../lib/components/repositoryCreateForm";
import {Error} from "../../lib/components/controls"
import {useAPI, useAPIWithPagination} from "../../lib/hooks/api";
import {Paginator} from "../../lib/components/pagination";
import Container from "react-bootstrap/Container";
import {Link} from "../../lib/components/nav";
import {useRouter} from "../../lib/hooks/router";

import {Route, Switch} from "react-router-dom";
import RepositoryPage from './repository';


const CreateRepositoryModal = ({show, error, onSubmit, onCancel}) => {

    const { response, error: err, loading } = useAPI(() => {
        return config.get()
    })

    const showError = (!!error) ? error : err
    if (loading)
        return (
            <Modal show={show} onHide={onCancel} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Create A New Repository</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Loading/>
                </Modal.Body>
            </Modal>
        )

    return (
        <Modal show={show} onHide={onCancel} size="lg">
            <Modal.Header closeButton>
                <Modal.Title>Create A New Repository</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <RepositoryCreateForm config={response} error={showError} onSubmit={onSubmit} onCancel={onCancel}/>
            </Modal.Body>
        </Modal>
    );
};



const RepositoryList = ({ onPaginate, prefix, after, refresh }) => {

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return repositories.list(prefix, after)
    }, [refresh, prefix, after])

    if (loading) return <Loading/>;
    if (!!error) return <Error error={error}/>

    return (
        <div>
            {results.map(repo => (
                <Row key={repo.id}>
                    <Col className={"mb-2 mt-2"}>
                        <Card>
                            <Card.Body>
                                <h5>
                                    <Link href={{
                                        pathname: `/repositories/:repoId/objects`,
                                        params: {repoId: repo.id}
                                    }}>
                                        {repo.id}
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

            <Paginator after={after} nextPage={nextPage} onPaginate={onPaginate}/>
        </div>
    );
};



const RepositoriesPage = () => {
    const router = useRouter();
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [createError, setCreateError] = useState(null);
    const [refresh, setRefresh] = useState(false);

    const routerPfx = (!!router.query.prefix) ? router.query.prefix : "";
    const [prefix, setPrefix] = useDebouncedState(
        routerPfx,
        (prefix) => router.push({pathname: `/repositories`, query: {prefix}})
    );

    const createRepo = async (repo) => {
        try {
            await repositories.create(repo)
            setRefresh(!refresh);
            setCreateError(null)
            router.push({pathname: `/repositories/:repoId/objects`, params: {repoId: repo.name}});
        } catch (error) {
            setCreateError(error);
        }
    }

    return (
        <Layout>
            <Container fluid="xl" className="mt-3">
                <ActionsBar>
                    <Form className="float-left" style={{minWidth: 300}} onSubmit={e => { e.preventDefault(); }}>
                        <Form.Row>
                            <Col>
                                <InputGroup>
                                    <InputGroup.Prepend>
                                        <InputGroup.Text>
                                            <SearchIcon/>
                                        </InputGroup.Text>
                                    </InputGroup.Prepend>
                                    <Form.Control
                                        placeholder="Find a repository..."
                                        autoFocus
                                        value={prefix}
                                        onChange={event => setPrefix(event.target.value)}
                                    />
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
                </ActionsBar>

                <RepositoryList
                    prefix={routerPfx}
                    refresh={refresh}
                    after={(!!router.query.after) ? router.query.after : ""}
                    onPaginate={after => {
                        const query = {after};
                        if (!!router.query.prefix) query.prefix = router.query.prefix;
                        router.push({pathname: `/repositories`, query});
                    }}
                    />

                <CreateRepositoryModal
                    onCancel={() => setShowCreateModal(false)}
                    show={showCreateModal}
                    error={createError}
                    onSubmit={(repo) => createRepo(repo)}/>
            </Container>
        </Layout>
    );
}

const RepositoriesIndex = () => {
    return (
        <Switch>
            <Route exact path="/repositories">
                <RepositoriesPage/>
            </Route>
            <Route path="/repositories/:repoId">
                <RepositoryPage/>
            </Route>
        </Switch>
    );
};

export default RepositoriesIndex;
