import React, {useCallback, useEffect, useState} from "react";

import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Modal from "react-bootstrap/Modal";
import Spinner from "react-bootstrap/Spinner";
import Stack from "react-bootstrap/Stack";

import {RepoIcon, SearchIcon} from "@primer/octicons-react";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";

import {ActionsBar, AlertError, Loading, useDebouncedState} from "../../lib/components/controls";
import {config, repositories} from '../../lib/api';
import {RepositoryCreateForm} from "../../lib/components/repositoryCreateForm";
import {useAPI, useAPIWithPagination} from "../../lib/hooks/api";
import {Paginator} from "../../lib/components/pagination";
import Container from "react-bootstrap/Container";
import {Link} from "../../lib/components/nav";
import {useRouter} from "../../lib/hooks/router";
import {ReadOnlyBadge} from "../../lib/components/badges";

import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";

dayjs.extend(relativeTime);

const LOCAL_BLOCKSTORE_TYPE = "local";
const LOCAL_BLOCKSTORE_SAMPLE_REPO_NAME = "quickstart";
const LOCAL_BLOCKSTORE_SAMPLE_REPO_DEFAULT_BRANCH = "main";

const CreateRepositoryButton = ({variant = "success", enabled = false, onClick}) => {
    return (
        <Button variant={variant} disabled={!enabled} onClick={onClick}>
            <RepoIcon/> Create Repository
        </Button>
    );
}

const GettingStartedCreateRepoButton = ({text, variant = "success", enabled = false, onClick, creatingRepo, style = {}}) => {
    return (
        <Button className="create-sample-repo-button" style={style} variant={variant} disabled={!enabled || creatingRepo} onClick={onClick}>
            { creatingRepo && <><Spinner as="span" role="status" aria-hidden="true" animation="border" size="sm" className="me-2"/><span className="visually-hidden">Loading...</span></> }
            {text}
        </Button>
    );
}

const CreateRepositoryModal = ({show, error, onSubmit, onCancel, inProgress, samlpleRepoChecked = false }) => {

  const [formValid, setFormValid] = useState(false);

  const { response, error: err, loading } = useAPI(() => config.getStorageConfig());

    const showError = (error) ? error : err;
    if (loading) {
        return (
            <Modal show={show} onHide={onCancel} size="lg">
                <Modal.Body>
                    <Loading/>
                </Modal.Body>
            </Modal>
        );
    }

    return (
        <Modal show={show} onHide={onCancel} size="lg">
            <Modal.Body>
                <RepositoryCreateForm
                  id="repository-create-form"
                  config={response}
                  error={showError}
                  formValid={formValid}
                  setFormValid={setFormValid}
                  onSubmit={onSubmit}
                  onCancel={onCancel}
                  inProgress={inProgress}
                  sampleRepoChecked={samlpleRepoChecked}
                />
            </Modal.Body>
            <Modal.Footer>
              <Button variant="success" type="submit" form="repository-create-form" className="me-2" disabled={!formValid || inProgress}>
                { inProgress ? 'Creating...' : 'Create Repository' }
              </Button>
              <Button variant="secondary" onClick={(e) => {
                e.preventDefault();
                onCancel();
              }}>Cancel</Button>
            </Modal.Footer>
        </Modal>
    );
};

const GetStarted = ({onCreateSampleRepo, onCreateEmptyRepo, creatingRepo, createRepoError }) => {
    return (
        <Card className="getting-started-card">
            <h2 className="main-title">Welcome to lakeFS!</h2>
            <Row className="text-container">
                <Col>
                    <p>{`To get started, create your first sample repository.`}<br />
                    {`This includes sample data, quickstart instructions, and more!`} <br />
                    {`Let's dive in 🤿`}</p>
                </Col>
            </Row>
            <Row className="button-container">
                <Col>
                    <GettingStartedCreateRepoButton text={
                      <><span>Create Sample Repository</span> </>
                    } creatingRepo={creatingRepo} variant={"success"} enabled={true} onClick={onCreateSampleRepo} />
                </Col>
            </Row>
            {createRepoError &&
                <Row>
                    <Col sm={6}>
                        <Alert className="mb-3" variant={"danger"}>{createRepoError.message}</Alert>
                    </Col>
                </Row>
            }

            <div className="d-flex flex-direction-row align-items-center">
                <span className="learn-more">Already working with lakeFS and just need an empty repository?</span>
                <GettingStartedCreateRepoButton style={{ padding: 0, width: "auto", marginLeft: "8px", display: "inline-block" }} text="Click here" variant={"link"} enabled={true} onClick={onCreateEmptyRepo} />
            </div>

            <img src="/getting-started.png" alt="getting-started" className="getting-started-image" />
        </Card>
    );
};

const RepositoryList = ({ onPaginate, prefix, after, refresh, onCreateSampleRepo, onCreateEmptyRepo, toggleShowActionsBar, creatingRepo, createRepoError }) => {

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return repositories.list(prefix, after);
    }, [refresh, prefix, after]);
    useEffect(() => {
      toggleShowActionsBar();
    }, [toggleShowActionsBar]);
    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;
    if (!after && !prefix && results.length === 0) {
        return <GetStarted onCreateSampleRepo={onCreateSampleRepo} onCreateEmptyRepo={onCreateEmptyRepo} creatingRepo={creatingRepo} createRepoError={createRepoError}/>;
    }

    return (
        <div>
            {results.map(repo => (
                <Row key={repo.id} className="repository-item">
                    <Col className={"mb-2 mt-2"}>
                        <Card>
                            <Card.Body>
                                <Stack direction="horizontal" gap={2} className="d-flex align-items-center">
                                    <h5>
                                        <Link href={{
                                            pathname: `/repositories/:repoId/objects`,
                                            params: {repoId: repo.id}
                                        }}>
                                            {repo.id}
                                        </Link>
                                    </h5>
                                    <ReadOnlyBadge readOnly={repo?.read_only} style={{ marginBottom: 5 }} />
                                </Stack>
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
    const [showActionsBar, setShowActionsBar] = useState(false);

    const routerPfx = (router.query.prefix) ? router.query.prefix : "";
    const [prefix, setPrefix] = useDebouncedState(
        routerPfx,
        (prefix) => router.push({pathname: `/repositories`, query: {prefix}})
    );

    const { response, error: err, loading } = useAPI(() => config.getStorageConfig());

    const createRepo = async (repo, presentRepo = true) => {
        try {
            setCreatingRepo(true);
            setCreateRepoError(null);
            await repositories.create(repo);
            setRefresh(!refresh);
            if (presentRepo) {
                router.push({pathname: `/repositories/:repoId/objects`, params: {repoId: repo.name}});
            }
            return true;
        } catch (error) {
            setCreatingRepo(false);
            setCreateRepoError(error);
            return false;
        }
    };

    const toggleShowActionsBar = useCallback((show = true) => {
        setShowActionsBar(show);
    }, [setShowActionsBar]);

    const createRepositoryButtonCallback = useCallback(() => {
        setSampleRepoChecked(false);
        setShowCreateRepositoryModal(true);
        setCreateRepoError(null);
    }, [showCreateRepositoryModal, setShowCreateRepositoryModal]);

    const createSampleRepoButtonCallback = useCallback(async () => {
        if (loading) return;
        if (!err && response?.blockstore_type === LOCAL_BLOCKSTORE_TYPE) {
            const sampleRepo = {
                name: LOCAL_BLOCKSTORE_SAMPLE_REPO_NAME,
                storage_namespace: `local://${LOCAL_BLOCKSTORE_SAMPLE_REPO_NAME}`,
                default_branch: LOCAL_BLOCKSTORE_SAMPLE_REPO_DEFAULT_BRANCH,
                sample_data: true,
            }
    
            await createRepo(sampleRepo);
            return;
        }
        setSampleRepoChecked(true);
        setShowCreateRepositoryModal(true);
        setCreateRepoError(null);
    }, [showCreateRepositoryModal, setShowCreateRepositoryModal, loading, err, response, createRepo]);

    return (
        <Container fluid="xl" className="mt-3">
            {showActionsBar && <ActionsBar>
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
                    <CreateRepositoryButton variant={"success"} enabled={true} onClick={createRepositoryButtonCallback} />
                </ButtonToolbar>
            </ActionsBar> }

                <RepositoryList
                    prefix={routerPfx}
                    refresh={refresh}
                    after={(router.query.after) ? router.query.after : ""}
                    onPaginate={after => {
                        const query = {after};
                        if (router.query.prefix) query.prefix = router.query.prefix;
                        router.push({pathname: `/repositories`, query});
                    }}
                    onCreateSampleRepo={createSampleRepoButtonCallback}
                    onCreateEmptyRepo={createRepositoryButtonCallback}
                    toggleShowActionsBar={toggleShowActionsBar}
                    creatingRepo={creatingRepo}
                    createRepoError={createRepoError}
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
    );
}

export default RepositoriesPage;
