import React, {useState} from "react";
import StepWizard from "react-step-wizard";
import {ProgressBar} from "react-bootstrap";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

const transitionDefaults = {
    enterRight: "",
    enterLeft: "",
    exitRight: "",
    exitLeft: "",
    intro: "",
}

const Wizard = ({
                    hashEnabled=false,
                    transitions=transitionDefaults,
                    showProgressBar = false,
                    showSkipButton = false,
                    onComplete = () => {},
                    canProceed = true,
                    onNextStep = () => {},
                    children,
                }) => {

    const [state, setState] = useState({
        stepWizard: {},
    });
    const [refresh, setRefresh] = useState(false);

    const onStepChange = () => {
        setRefresh(!refresh);
        onNextStep();
    };

    const setInstance = stepWizard => {
        setState({...state, stepWizard: stepWizard});
    }

    return (
        <Container className='container'>
            <div className='jumbotron'>
                <Row className={'justify-content-center'}>
                    <Col>
                        <StepWizard
                            onStepChange={onStepChange}
                            transitions={transitions}
                            nav={showProgressBar && <WizardNav />}
                            instance={setInstance}
                            isHashEnabled={hashEnabled}
                        >
                            {children}
                        </StepWizard>
                    </Col>
                </Row>
            </div>

            {<WizardController
                stepWizard={state.stepWizard}
                canProceed={canProceed}
                skipButton={showSkipButton}
                onComplete={onComplete}/>}

        </Container>
    );
};

const WizardNav = ({totalSteps, currentStep}) => {
    return (
        <ProgressBar className={'wizard-progress-bar'} striped max={totalSteps} min={1} now={currentStep} />
    );
}

const WizardController = ({stepWizard, canProceed, skipButton = false, onComplete}) => {
    return (
        <Container>
            <Row className={'justify-content-center'}>
                {
                    skipButton && stepWizard.currentStep < stepWizard.totalSteps ?
                        <Col className={"col-1 mb-2 mt-2"}>
                            <button className='btn btn-secondary btn-block' onClick={stepWizard.nextStep}>Skip</button>
                        </Col>
                        :
                        null
                }
                <Col className={"col-2 mb-2 mt-2"}>
                    {
                        stepWizard.currentStep < stepWizard.totalSteps ?
                            <button className='btn btn-primary btn-block' disabled={!canProceed} onClick={stepWizard.nextStep}>Next Step</button>
                            :
                            <button className='btn btn-success btn-block' disabled={!canProceed} onClick={onComplete} >Finish</button>
                    }
                </Col>
            </Row>
        </Container>
    );
}

export default Wizard;
