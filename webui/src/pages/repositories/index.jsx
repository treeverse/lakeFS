import React, {useCallback, useState} from "react";

import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Modal from "react-bootstrap/Modal";

import {RepoIcon, SearchIcon, BeakerIcon} from "@primer/octicons-react";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";

import Layout from "../../lib/components/layout";
import {ActionsBar, Error, Loading, useDebouncedState} from "../../lib/components/controls";
import {config, repositories} from '../../lib/api';
import {RepositoryCreateForm} from "../../lib/components/repositoryCreateForm";
import {useAPI, useAPIWithPagination} from "../../lib/hooks/api";
import {Paginator} from "../../lib/components/pagination";
import Container from "react-bootstrap/Container";
import {Link} from "../../lib/components/nav";
import {useRouter} from "../../lib/hooks/router";

import {Route, Routes} from "react-router-dom";
import RepositoryPage from './repository';
import Button from "react-bootstrap/Button";

dayjs.extend(relativeTime);

const CreateRepositoryButton = ({variant = "success", enabled = false, onClick}) => {
    return (
        <Button variant={variant} disabled={!enabled} onClick={onClick}>
            <RepoIcon/> Create Repository
        </Button>
    );
}

const CreateSampleRepoButton = ({variant = "success", enabled = false, onClick}) => {
    return (
        <Button className="create-sample-repo-button d-flex align-items-center" variant={variant} disabled={!enabled} onClick={onClick}>
            <BeakerIcon className="me-2" /> Create Sample Repository
        </Button>
    );
}

const CreateRepositoryModal = ({show, error, onSubmit, onCancel, inProgress, samlpleRepoChecked = false }) => {

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
                <RepositoryCreateForm config={response} error={showError} onSubmit={onSubmit} onCancel={onCancel} inProgress={inProgress} samlpleRepoChecked={samlpleRepoChecked} />
            </Modal.Body>
        </Modal>
    );
};

const GetStarted = ({onCreateSampleRepo}) => {
    return (
        <Card className="getting-started-card">
            <h2 className="main-title">Welcome to lakeFS!</h2>
            <Row className="text-container">
                <Col>
                    <h4>New here? Not sure where to start?</h4>
                    <p>{`It's ok, we've all been there.`} <br />
                    {`A great place to start is with a sample repository.`} <br />
                    {`It includes sample data, sample hooks, and a README to guide you through your first steps.`}</p>
                </Col>
            </Row>
            <Row className="button-container">
                <Col>
                    <CreateSampleRepoButton variant={"success"} enabled={true} onClick={onCreateSampleRepo} />
                </Col>
            </Row>
            <Row>
                <Col>
                    <span className="learn-more">Want to learn more about lakeFS? Find out how <a href="https://www.google.com" target="_blank" rel="noreferrer">repositories</a> work.</span>
                </Col>
            </Row>
            <img src="/getting-started.svg" alt="getting-started" className="getting-started-image" />
        </Card>
    );
};

const RepositoryList = ({ onPaginate, prefix, after, refresh, onCreateSampleRepo }) => {

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return repositories.list(prefix, after);
    }, [refresh, prefix, after]);

    if (loading) return <Loading/>;
    if (error) return <Error error={error}/>;
    if (!after && !prefix && results.length === 0) {
        return <GetStarted onCreateSampleRepo={onCreateSampleRepo}/>;
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
                                        created at <code>{dayjs.unix(repo.creation_date).toISOString()}</code> ({dayjs.unix(repo.creation_date).fromNow()})<br/>
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
    const [sampleRepoChecked, setSampleRepoChecked] = useState(false);
    const [createRepoError, setCreateRepoError] = useState(null);
    const [refresh, setRefresh] = useState(false);
    const [creatingRepo, setCreatingRepo] = useState(false);

    const routerPfx = (router.query.prefix) ? router.query.prefix : "";
    const [prefix, setPrefix] = useDebouncedState(
        routerPfx,
        (prefix) => router.push({pathname: `/repositories`, query: {prefix}})
    );

    const CreateRepositoryButtonCallback = useCallback(() => {
        setSampleRepoChecked(false);
        setShowCreateRepositoryModal(true);
        setCreateRepoError(null);
    }, [showCreateRepositoryModal, setShowCreateRepositoryModal]);

    const CreateSampleRepoButtonCallback = useCallback(() => {
        setSampleRepoChecked(true);
        setShowCreateRepositoryModal(true);
        setCreateRepoError(null);
    }, [showCreateRepositoryModal, setShowCreateRepositoryModal]);

    const createRepo = async (repo, presentRepo = true) => {
        try {
            setCreatingRepo(true);
            await repositories.create(repo);
            setRefresh(!refresh);
            setCreateRepoError(null);
            if (presentRepo) {
                router.push({pathname: `/repositories/:repoId/objects`, params: {repoId: repo.name}});
            }
            return true;
        } catch (error) {
            setCreatingRepo(false);
            setCreateRepoError(error);
            return false;
        }
    }

    return (
        <Layout>
            <Container fluid="xl" className="mt-3">
                <ActionsBar>
                    <Form style={{minWidth: 300}} onSubmit={e => { e.preventDefault(); }}>
                        <Form.Group>
                            <Col>
                                <InputGroup>
                                    <InputGroup.Text>
                                        <SearchIcon/>
                                    </InputGroup.Text>
                                    <Form.Control
                                        placeholder="Find a repository..."
                                        autoFocus
                                        value={prefix}
                                        onChange={event => setPrefix(event.target.value)}
                                    />
                                </InputGroup>
                            </Col>
                        </Form.Group>
                    </Form>
                    <ButtonToolbar className="ms-auto mb-2">
                        <CreateRepositoryButton variant={"success"} enabled={true} onClick={CreateRepositoryButtonCallback} />
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
                    onCreateSampleRepo={CreateSampleRepoButtonCallback}
                    />

                <CreateRepositoryModal
                    onCancel={() => {
                        setShowCreateRepositoryModal(false);
                        setCreateRepoError(null);
                    }}
                    show={showCreateRepositoryModal}
                    error={createRepoError}
                    onSubmit={(repo) => createRepo(repo, true)}
                    samlpleRepoChecked={sampleRepoChecked}
                    inProgress={creatingRepo}
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

const RepositoriesIndex = () => {
    return (
        <Routes>
            <Route path="/" element={<RepositoriesPage/>} />
            <Route path=":repoId/*" element={<RepositoryPage/>} />
        </Routes>
    );
};

export default RepositoriesIndex;
