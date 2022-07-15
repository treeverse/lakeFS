import Wizard from "./wizard";
import React, {useState} from "react";
import {useAPI} from "../../../lib/hooks/api";
import {config} from "../../../lib/api";
import {Loading} from "../../../lib/components/controls";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import {Spinner} from "react-bootstrap";
import Alert from "react-bootstrap/Alert";
import RepositoryCreateForm from "../../../lib/components/repositoryCreateForm";

const RepositoryCreationPhase = {
    NotStarted: 0,
    InProgress: 1,
    Completed: 2,
    Failed: 3,
}

const RepositoryCreationStep = ({repoCreationError, createRepo, onCancel, onComplete}) => {
    const {response, error: err, loading} = useAPI(() => config.getStorageConfig());
    const [repoName, setRepoName] = useState('');
    const [repoCreationPhase, setRepoCreationPhase] = useState(RepositoryCreationPhase.NotStarted);

    const onSubmit = async (repo) => {
        setRepoCreationPhase(RepositoryCreationPhase.InProgress);
        const success = await createRepo(repo, false);
        if (success) {
            setRepoCreationPhase(RepositoryCreationPhase.Completed);
            onComplete({ 'branch': repo.default_branch, 'namespace': repo.storage_namespace },);
            setRepoName(repo.name);
        }
        else {
            setRepoCreationPhase(RepositoryCreationPhase.Failed);
        }
    }
    const showError = repoCreationError ? repoCreationError : err;
    let present;
    if (loading) {
        present = <Loading/>;
    } else if (repoCreationPhase === RepositoryCreationPhase.InProgress) {
        present = <Container>
            <Row className={'justify-content-left'}>
                <Col className={"col-1 mb-2 mt-2"}>
                    <Spinner animation="border" variant="primary" size="xl" role="status" />
                </Col>
                <Col className={"col-3 mb-2 mt-2"}>
                    <span>Creating repository...</span>
                </Col>
            </Row>
        </Container>;
    } else if (repoCreationPhase === RepositoryCreationPhase.Completed) {
        present = <Alert variant="info" className={"mt-3"}>Repository <span className={"font-weight-bold"}>{repoName}</span> created successfully</Alert>;
    }
    else {
        present = <RepositoryCreateForm config={response} error={showError} onSubmit={onSubmit} onCancel={onCancel}/>;
    }
    return (
        <>
            <h2 className={"wizard-step-header"}>Create a repository</h2>
            {present}
        </>
    );
}

export const SparkQuickstart = ({onExit, createRepo, repoCreationError}) => {
    const [nextEnabled, setNextEnabled] = useState(false)
    const [state, setState] = useState({});
    const completedStep = (val = {}) => {
        setState({...state, ...val});
        setNextEnabled(true);
    }
    const onComplete = () => {
        onExit();
    }
    return (
        <Wizard
            showSkipButton
            canProceed={nextEnabled}
            onNextStep={() => setNextEnabled(false)}
            onComplete={onComplete}
            showProgressBar={true}>

            <RepositoryCreationStep
                repoCreationError={repoCreationError}
                createRepo={createRepo}
                onCancel={onExit}
                onComplete={completedStep} />
        </Wizard>
    );
}
