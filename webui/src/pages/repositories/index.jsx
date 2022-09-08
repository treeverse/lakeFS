import React, {useState} from "react";

import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Modal from "react-bootstrap/Modal";

import {RepoIcon, SearchIcon} from "@primer/octicons-react";
import moment from "moment";

import Layout from "../../lib/components/layout";
import {ActionsBar, Error, ExitConfirmationDialog, Loading, useDebouncedState} from "../../lib/components/controls";
import {config, repositories} from '../../lib/api';
import {RepositoryCreateForm} from "../../lib/components/repositoryCreateForm";
import {useAPI, useAPIWithPagination} from "../../lib/hooks/api";
import {Paginator} from "../../lib/components/pagination";
import Container from "react-bootstrap/Container";
import {Link} from "../../lib/components/nav";
import {useRouter} from "../../lib/hooks/router";

import {Route, Switch} from "react-router-dom";
import RepositoryPage from './repository';
import Alert from "react-bootstrap/Alert";
import Dropdown from "react-bootstrap/Dropdown";

import {SparkQuickstart} from "./wizard/spark_quickstart_wizard";


const CreateRepositoryModal = ({show, error, onSubmit, onCancel}) => {

    const { response, error: err, loading } = useAPI(() => config.getStorageConfig());

    const showError = (error) ? error : err;
    if (loading)
        return (
            <Modal show={show} onHide={onCancel} size="lg">
                <Modal.Header closeButton>
                    <ModalTitleContainer/>
                </Modal.Header>
                <Modal.Body>
                    <Loading/>
                </Modal.Body>
            </Modal>
        );

    return (
        <Modal show={show} onHide={onCancel} size="lg">
            <Modal.Header closeButton>
                <ModalTitleContainer/>
            </Modal.Header>
            <Modal.Body>
                <RepositoryCreateForm config={response} error={showError} onSubmit={onSubmit} onCancel={onCancel}/>
            </Modal.Body>
        </Modal>
    );
};

const RepositoryTemplatesModal = ({show, onExit, createRepo, repoCreationError}) => {
    const [isExitDialogOpen, setIsExitDialogOpen] = useState(false);
    const onHide = () => {
        setIsExitDialogOpen(true);
    }
    return (
        <>
            <ExitConfirmationDialog
                dialogAlert={'Are you sure you want to exit?'}
                dialogDescription={'If you stop the Spark quickstart wizard in the middle of the process, you might get partial results.'}
                onExit={() => {
                    setIsExitDialogOpen(false);
                    onExit();
                }}
                onContinue={() => setIsExitDialogOpen(false)}
                isOpen={isExitDialogOpen}
            />
            <Modal show={show} onHide={onHide} className={"wizard-modal"} size="lg">
                <Modal.Header closeButton>
                    <TemplatesModalTitleContainer/>
                </Modal.Header>
                <Modal.Body>
                    <SparkQuickstart
                        onExit={onExit}
                        createRepo={createRepo}
                        repoCreationError={repoCreationError}
                    />
                </Modal.Body>
            </Modal>
        </>
    );
};


const GetStarted = ({onCreateRepo}) => {
    return (
        <Alert variant={"secondary"}>
            <h4>You don&apos;t have any repositories yet.</h4>
            {/* eslint-disable-next-line react/jsx-no-target-blank */}
            <Link onClick={onCreateRepo} href="#">Create your first repository</Link> or <a
            href="https://docs.lakefs.io/understand/model.html#repository" target="_blank">learn more about
            repositories in lakeFS</a>.
        </Alert>
    );
};

const RepositoryList = ({ onPaginate, prefix, after, refresh, onCreateRepo }) => {

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return repositories.list(prefix, after)
    }, [refresh, prefix, after])

    if (loading) return <Loading/>;
    if (error) return <Error error={error}/>
    if (!after && !prefix && results.length === 0) {
        return <GetStarted onCreateRepo={onCreateRepo}/>;
    }

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
    const [showCreateRepositoryModal, setShowCreateRepositoryModal] = useState(false);
    const [showRepositoryTemplatesModal, setShowRepositoryTemplatesModal] = useState(false);
    const [createRepoError, setCreateRepoError] = useState(null);
    const [refresh, setRefresh] = useState(false);

    const routerPfx = (router.query.prefix) ? router.query.prefix : "";
    const [prefix, setPrefix] = useDebouncedState(
        routerPfx,
        (prefix) => router.push({pathname: `/repositories`, query: {prefix}})
    );

    const createRepo = async (repo, presentRepo = true) => {
        try {
            await repositories.create(repo);
            setRefresh(!refresh);
            setCreateRepoError(null);
            if (presentRepo) {
                router.push({pathname: `/repositories/:repoId/objects`, params: {repoId: repo.name}});
            }
            return true;
        } catch (error) {
            setCreateRepoError(error);
            return false;
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
                        <Dropdown>
                            <Dropdown.Toggle variant="success" id="template-picker-dropdown">
                                <RepoIcon/> Create Repository
                            </Dropdown.Toggle>
                            <Dropdown.Menu>
                                <Dropdown.Item onClick={() => {
                                    setShowCreateRepositoryModal(true);
                                    setCreateRepoError(null);
                                }}>Blank Repository</Dropdown.Item>
                                <Dropdown.Item onClick={() => {
                                    setShowRepositoryTemplatesModal(true);
                                    setCreateRepoError(null);
                                }}>Spark Quickstart</Dropdown.Item>
                            </Dropdown.Menu>
                        </Dropdown>
                    </ButtonToolbar>
                </ActionsBar>

                <RepositoryList
                    prefix={routerPfx}
                    refresh={refresh}
                    after={(router.query.after) ? router.query.after : ""}
                    onPaginate={after => {
                        const query = {after};
                        if (router.query.prefix) query.prefix = router.query.prefix;
                        router.push({pathname: `/repositories`, query});
                    }}
                    onCreateRepo={() => setShowCreateRepositoryModal(true)}
                    />

                <CreateRepositoryModal
                    onCancel={() => {
                        setShowCreateRepositoryModal(false);
                        setCreateRepoError(null);
                    }}
                    show={showCreateRepositoryModal}
                    error={createRepoError}
                    onSubmit={(repo) => createRepo(repo, true)}/>

                <RepositoryTemplatesModal
                    onExit={() => {
                        setShowRepositoryTemplatesModal(false);
                        setCreateRepoError(null);
                    }}
                    show={showRepositoryTemplatesModal}
                    createRepo={(repo) => createRepo(repo, false)}
                    repoCreationError={createRepoError}
                />
            </Container>
        </Layout>
    );
}

const ModalTitleContainer = () => {
    return (
        <Container fluid="true" className="justify-content-start">
            <Row>
                <Col>
                    <Modal.Title>Create A New Repository</Modal.Title>
                </Col>
            </Row>
            <Row>
                <Col>
                    A repository contains all of your objects, including the revision history. <a href="https://docs.lakefs.io/understand/model.html#repository" target="_blank" rel="noopener noreferrer">Learn more.</a>
                </Col>
            </Row>
        </Container>
    );
};

const TemplatesModalTitleContainer = () => {
    return (
        <Modal.Title>Spark Quickstart</Modal.Title>
    );
};

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
