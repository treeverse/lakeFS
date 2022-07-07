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

const RepositoryCreationStep = ({repoCreationError, createRepo, onCancel, onComplete}) => {
    const {response, error: err, loading} = useAPI(() => config.getStorageConfig());
    const [repoName, setRepoName] = useState("");
    const [creationInProgress, setCreationInProgress] = useState(false);

    const onSubmit = async (repo) => {
        setCreationInProgress(true);
        const success = await createRepo(repo, false);
        setCreationInProgress(false);
        console.log(`Success is: ${success}`);
        if(success) {
            onComplete({ 'branch': repo.default_branch, 'namespace': repo.storage_namespace },);
            console.log(`Setting repo name to: ${repo.name}`);
            setRepoName(repo.name);
            console.log(`Set repo name to: ${repoName}`);
        }
    }
    const showError = (repoCreationError) ? repoCreationError : err;
    let present;
    if (loading) {
        present = <Loading/>;
    } else if(creationInProgress) {
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
    } else if (repoName) {
        present = <Alert variant="info" className={"mt-3"}>Repository {repoName} successfully created.</Alert>;
    }
    else {
        present = <RepositoryCreateForm config={response} error={showError} onSubmit={onSubmit} onCancel={onCancel}/>;
    }
    console.log(`current repo name: ${repoName}`);
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
    const onDone = () => {
        onExit();
    }
    return (
        <Wizard
            showSkipButton
            canProceed={nextEnabled}
            onNextStep={() => setNextEnabled(false)}
            onComplete={onDone}
            showProgressBar={true}>

            <RepositoryCreationStep
                repoCreationError={repoCreationError}
                createRepo={createRepo}
                onCancel={onExit}
                onComplete={completedStep} />
        </Wizard>
    );
}
