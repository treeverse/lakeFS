import {Wizard} from "./wizard";
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
import ImportDataStep from "./import_data_wizard_step";

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
            onComplete({ 'branch': repo.default_branch, 'namespace': repo.storage_namespace, 'repoId': repo.name },);
            setRepoName(repo.name);
        }
        else {
            setRepoCreationPhase(RepositoryCreationPhase.Failed);
        }
    }
    const showError = repoCreationError ? repoCreationError : err;
    let content;
    if (loading) {
        content = <Loading/>;
    } else if (repoCreationPhase === RepositoryCreationPhase.InProgress) {
        content = <ProgressSpinner text={'Creating repository...'} />;
    } else if (repoCreationPhase === RepositoryCreationPhase.Completed) {
        content = <Alert variant="info" className={"mt-3"}>Repository <span className={"font-weight-bold"}>{repoName}</span> created successfully</Alert>;
    }
    else {
        content = <RepositoryCreateForm config={response} error={showError} onSubmit={onSubmit} onCancel={onCancel} sm={8}/>;
    }
    return (
        <>
            <h3 className={"wizard-step-header"}>Create a repository</h3>
            {content}
        </>
    );
}

export const SparkQuickstart = ({onExit, createRepo, repoCreationError}) => {
    const [state, setState] = useState({
        branch: '',
        namespace: '',
        repoId: '',
    });
    const [completed, setCompleted] = useState(new Set());
    const completedStep = (vals = {}, stepNum) => {
        setState({...state, ...vals});
        setCompleted(currentCompleted => new Set(currentCompleted).add(stepNum));
    }
    const onComplete = async () => {
        onExit();
    }
    const steps = [
        {
            label: 'Create Repository',
            component: <RepositoryCreationStep
                stepNum
                repoCreationError={repoCreationError}
                createRepo={createRepo}
                onCancel={onExit}
                onComplete={(values) => {
                    completedStep(values, 0);
                }} />,
        },
        {
            label: 'Import Data',
            optional: true,
            component: <ImportDataStep
                repoId={state.repoId}
                onComplete={(values) => {
                    completedStep(values, 1);
                }}
                branchName={state.branch} />,
        },
    ];
    return (
        <>
            <Wizard steps={steps} isShowBack={false} completed={completed} onDone={onComplete}/>
        </>
    );
}

const ProgressSpinner = ({text, changingElement =''}) => {
    return (
        <Container>
            <Row className={'justify-content-left'}>
                <Col className={"col-1 mb-2 mt-2"}>
                    <Spinner animation="border" variant="primary" size="xl" role="status" />
                </Col>
                <Col className={"col-3 mb-2 mt-2"}>
                    <span>{text}{changingElement}</span>
                </Col>
            </Row>
        </Container>
    );
}
