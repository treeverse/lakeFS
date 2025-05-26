import React, {useCallback, useEffect, useState} from "react";

import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Card from "react-bootstrap/Card";
import InputGroup from "react-bootstrap/InputGroup";
import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Modal from "react-bootstrap/Modal";
import Spinner from "react-bootstrap/Spinner";

import {RepoIcon, SearchIcon} from "@primer/octicons-react";
import dayjs from "dayjs";
import relativeTime from "dayjs/plugin/relativeTime";

import {ActionsBar, AlertError, Loading, useDebouncedState} from "../../lib/components/controls";
import {config, repositories} from '../../lib/api';
import {useAPI, useAPIWithPagination} from "../../lib/hooks/api";
import {Paginator} from "../../lib/components/pagination";
import Container from "react-bootstrap/Container";
import {Link} from "../../lib/components/nav";
import {useRouter} from "../../lib/hooks/router";
import {ReadOnlyBadge} from "../../lib/components/badges";

import Button from "react-bootstrap/Button";
import Alert from "react-bootstrap/Alert";
import {usePluginManager} from "../../extendable/plugins/pluginsContext";

dayjs.extend(relativeTime);

const LOCAL_BLOCKSTORE_TYPE = "local";
const LOCAL_BLOCKSTORE_SAMPLE_REPO_NAME = "quickstart";
const LOCAL_BLOCKSTORE_SAMPLE_REPO_DEFAULT_BRANCH = "main";

const CreateRepositoryButton = ({variant = "success", enabled = false, onClick}) => {
    return (
        <Button variant={variant} disabled={!enabled} onClick={onClick} className="d-flex align-items-center">
            <RepoIcon className="me-2"/> Create Repository
        </Button>
    );
}

const GettingStartedCreateRepoButton = ({text, variant = "success", enabled = false, onClick, creatingRepo, className = ""}) => {
    return (
        <Button className={`create-sample-repo-button ${className}`} variant={variant} disabled={!enabled || creatingRepo} onClick={onClick}>
            { creatingRepo && <><Spinner as="span" role="status" aria-hidden="true" animation="border" size="sm" className="me-2"/><span className="visually-hidden">Loading...</span></> }
            {text}
        </Button>
    );
}

const CreateRepositoryModal = ({show, error, onSubmit, onCancel, inProgress}) => {
    const pluginManager = usePluginManager();
    const repoCreationFormPlugin = pluginManager.repoCreationForm

    const [formValid, setFormValid] = useState(false);

    const {response: storageConfigs, error: err, loading} = useAPI(() => config.getStorageConfigs());

    const buttonContent = inProgress ? (
        <>
            <Spinner as="span" size="sm" animation="border" role="status" className="me-2" />
            Creating...
        </>
    ) : 'Create Repository';

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
                {repoCreationFormPlugin.build({
                    formID: "repository-create-form",
                    configs: storageConfigs,
                    error: showError,
                    formValid,
                    setFormValid,
                    onSubmit,
                })}
            </Modal.Body>
            <Modal.Footer className="border-0 pt-0">
                <Button 
                    variant="outline-secondary" 
                    onClick={(e) => {
                        e.preventDefault();
                        onCancel();
                    }}
                >
                    Cancel
                </Button>
                <Button 
                    variant="success" 
                    type="submit" 
                    form="repository-create-form" 
                    className="ms-2"
                    disabled={!formValid || inProgress}
                >
                    {buttonContent}
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

const GetStarted = ({allowSampleRepoCreation, onCreateSampleRepo, onCreateEmptyRepo, creatingRepo, createRepoError }) => {
    return (
        <Card className="getting-started-card">
            <h2 className="main-title">Welcome to lakeFS!</h2>
            <Row className="text-container">
                <Col md={7}>
                    <p className="lead mb-4">
                        Create your first sample repository to get started with lakeFS. 
                        This includes sample data, quickstart instructions, and everything 
                        you need to explore lakeFS capabilities.
                    </p>
                    
                    {allowSampleRepoCreation && (
                        <div className="mb-4">
                            <GettingStartedCreateRepoButton 
                                text={<span>Create Sample Repository</span>}
                                creatingRepo={creatingRepo} 
                                variant={"success"} 
                                enabled={true} 
                                onClick={onCreateSampleRepo}
                            />
                        </div>
                    )}
                    
                    {createRepoError && (
                        <Alert className="mb-3" variant={"danger"}>
                            {createRepoError.message}
                        </Alert>
                    )}
                    
                    <div className="d-flex align-items-center mt-4">
                        <span className="learn-more">Already working with lakeFS?</span>
                        <GettingStartedCreateRepoButton 
                            className="inline-link-button"
                            text="Create an empty repository" 
                            variant={"link"} 
                            enabled={true} 
                            onClick={onCreateEmptyRepo} 
                        />
                    </div>
                </Col>
            </Row>

            <img src="/getting-started.png" alt="getting-started" className="getting-started-image" />
        </Card>
    );
};

const RepositoryList = ({ onPaginate, search, after, refresh, allowSampleRepoCreation, onCreateSampleRepo, onCreateEmptyRepo, toggleShowActionsBar, creatingRepo, createRepoError }) => {

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return repositories.list(search, after);
    }, [refresh, search, after]);
    useEffect(() => {
      toggleShowActionsBar();
    }, [toggleShowActionsBar]);
    if (loading) return <Loading/>;
    if (error) return <AlertError error={error}/>;
    if (!after && !search && results.length === 0) {
        return <GetStarted
            allowSampleRepoCreation={allowSampleRepoCreation}
            onCreateSampleRepo={onCreateSampleRepo}
            onCreateEmptyRepo={onCreateEmptyRepo}
            creatingRepo={creatingRepo}
            createRepoError={createRepoError}
        />;
    }

    return (
        <div>
            {results.map(repo => (
                <Row key={repo.id} className="repository-item">
                    <Col className={"mb-3 mt-1"}>
                        <Card className="h-100 repository-card">
                            <Card.Body className="d-flex flex-column p-3">
                                <div className="d-flex justify-content-between align-items-start">
                                    <div className="d-flex flex-column">
                                        <h5 className="repository-title mb-0">
                                            <Link href={{
                                                pathname: `/repositories/:repoId/objects`,
                                                params: {repoId: repo.id}
                                            }}>
                                                <div className="d-flex align-items-center">
                                                    <div className="repo-icon-wrapper me-2">
                                                        <RepoIcon size={16} />
                                                    </div>
                                                    <span>{repo.id}</span>
                                                </div>
                                            </Link>
                                        </h5>
                                        <div className="repository-created-date">
                                            <span title={dayjs.unix(repo.creation_date).toISOString()}>
                                                Created {dayjs.unix(repo.creation_date).fromNow()}
                                            </span>
                                        </div>
                                    </div>
                                    <div className="d-flex align-items-center">
                                        <ReadOnlyBadge readOnly={repo?.read_only} />
                                    </div>
                                </div>
                                
                                <div className="repository-details-compact mt-2">
                                    <div className="detail-row">
                                        <div className="detail-item-compact">
                                            <div className="detail-label-compact">Branch</div>
                                            <div className="detail-value-compact">
                                                <code>{repo.default_branch}</code>
                                            </div>
                                        </div>
                                        
                                        {repo.storage_id && repo.storage_id.length &&
                                            <div className="detail-item-compact">
                                                <div className="detail-label-compact">Storage</div>
                                                <div className="detail-value-compact">
                                                    <code>{repo.storage_id}</code>
                                                </div>
                                            </div>
                                        }
                                        
                                        <div className="detail-item-compact storage-namespace">
                                            <div className="detail-label-compact">Namespace</div>
                                            <div className="detail-value-compact">
                                                <code className="text-truncate">{repo.storage_namespace}</code>
                                            </div>
                                        </div>
                                    </div>
                                </div>
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
    const pluginManager = usePluginManager();
    const router = useRouter();
    const [showCreateRepositoryModal, setShowCreateRepositoryModal] = useState(false);
    const [createRepoError, setCreateRepoError] = useState(null);
    const [refresh, setRefresh] = useState(false);
    const [creatingRepo, setCreatingRepo] = useState(false);
    const [showActionsBar, setShowActionsBar] = useState(false);

    const routerPfx = (router.query.search) ? router.query.search : "";
    const [search, setSearch] = useDebouncedState(
        routerPfx,
        (search) => router.push({pathname: `/repositories`, query: {search}})
    );

    const { response: storageConfigs, error: err, loading } = useAPI(() => config.getStorageConfigs());

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
        setShowCreateRepositoryModal(true);
        setCreateRepoError(null);
    }, [showCreateRepositoryModal, setShowCreateRepositoryModal]);

    const allowSampleRepoCreation = pluginManager.repoCreationForm.allowSampleRepoCreationFunc(storageConfigs);
    const createSampleRepoButtonCallback = useCallback(async () => {
        if (loading) return;
        // note that this is only called on a single storage config server
        if (!err && storageConfigs.length && storageConfigs[0]?.blockstore_type === LOCAL_BLOCKSTORE_TYPE) {
            const sampleRepo = {
                name: LOCAL_BLOCKSTORE_SAMPLE_REPO_NAME,
                storage_namespace: `local://${LOCAL_BLOCKSTORE_SAMPLE_REPO_NAME}`,
                default_branch: LOCAL_BLOCKSTORE_SAMPLE_REPO_DEFAULT_BRANCH,
                sample_data: true,
            }
    
            await createRepo(sampleRepo);
            return;
        }
        setShowCreateRepositoryModal(true);
        setCreateRepoError(null);
    }, [showCreateRepositoryModal, setShowCreateRepositoryModal, loading, err, storageConfigs, createRepo]);

    return (
        <Container fluid="xl" className="mt-3">
            {showActionsBar && 
                <ActionsBar className="mb-4 p-3 bg-white rounded shadow-sm">
                    <Form style={{minWidth: 300}} onSubmit={e => { e.preventDefault(); }}>
                        <Form.Group>
                            <Col>
                                <InputGroup className="search-input-group">
                                    <InputGroup.Text className="bg-light border-end-0">
                                        <SearchIcon/>
                                    </InputGroup.Text>
                                    <Form.Control
                                        className="border-start-0 bg-light"
                                        placeholder="Search repositories..."
                                        autoFocus
                                        value={search}
                                        onChange={event => setSearch(event.target.value)}
                                    />
                                </InputGroup>
                            </Col>
                        </Form.Group>
                    </Form>
                    <ButtonToolbar className="ms-auto">
                        <CreateRepositoryButton variant={"success"} enabled={true} onClick={createRepositoryButtonCallback} />
                    </ButtonToolbar>
                </ActionsBar>
            }

                <RepositoryList
                    search={routerPfx}
                    refresh={refresh}
                    after={(router.query.after) ? router.query.after : ""}
                    onPaginate={after => {
                        const query = {after};
                        if (router.query.search) query.search = router.query.search;
                        router.push({pathname: `/repositories`, query});
                    }}
                    allowSampleRepoCreation={allowSampleRepoCreation}
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
                inProgress={creatingRepo}
                />

        </Container>
    );
}

export default RepositoriesPage;
