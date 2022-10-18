import React, {useState} from "react";
import {Box, Button, Step, StepLabel, Stepper, Typography} from "@mui/material";
import noop from "lodash/noop";

const defaultSteps = [{label: '', component: <></>, optional: false}];

export const Wizard = ({steps = defaultSteps, isShowBack= true, completed= {}, onDone = noop, isStepInProgress}) => {
    const [activeStep, setActiveStep] = useState(0);
    const [skipped, setSkipped] = useState(new Set());

    const isStepOptional = (stepIndex) => {
        return steps[stepIndex].optional;
    };

    const isStepSkipped = (stepIndex) => {
        return skipped.has(stepIndex);
    };

    const isStepCompleted = (stepIndex) => {
        return completed.has(stepIndex);
    }

    const shouldShowNextBeforeCompletion = (stepIndex) => {
        const shouldHide = steps[stepIndex].hideNextUntilCompletion;
        return !shouldHide || (shouldHide && isStepCompleted(stepIndex));
    }

    const handleNext = () => {
        if (activeStep === steps.length - 1) {
            onDone();
        }
        else {
            let newSkipped = skipped;
            if (isStepSkipped(activeStep)) {
                newSkipped = new Set(newSkipped.values());
                newSkipped.delete(activeStep);
            }
            setActiveStep((prevActiveStep) => prevActiveStep + 1);
            setSkipped(newSkipped);
        }
    };

    const handleBack = () => {
        setActiveStep((prevActiveStep) => prevActiveStep - 1);
    };

    const handleSkip = () => {
        if (!isStepOptional(activeStep)) {
            throw new Error("You can't skip a step that isn't optional.");
        }
        if (activeStep === steps.length - 1) {
            onDone();
        }
        else {
            setActiveStep((prevActiveStep) => prevActiveStep + 1);
            setSkipped((prevSkipped) => new Set(prevSkipped).add(activeStep));
        }
    };

    return (
        <Box className={'jumbotron'} sx={{mb: 0, pb: 3, pt: 5}}>
            <Stepper activeStep={activeStep} alternativeLabel>
                {steps.map((step, index) => {
                    const stepProps = {};
                    const labelProps = {};
                    if (step.optional) {
                        labelProps.optional = (
                            <Typography variant="caption">Optional</Typography>
                        );

                    }
                    if (isStepSkipped(index)) {
                        stepProps.completed = false;
                    }
                    return (
                        <Step key={step.label} {...stepProps}>
                            <StepLabel align={"center"} {...labelProps}>{step.label}</StepLabel>
                        </Step>
                    );
                })}
            </Stepper>
            <>
                <Box sx={{ mt: 2, mb: 1 }}>
                    {steps[activeStep].component}
                </Box>
                <Box sx={{ display: 'flex', flexDirection: 'row', pt: 2 }}>
                    {isShowBack && <Button
                        color="inherit"
                        disabled={activeStep === 0}
                        onClick={handleBack}
                        sx={{mr: 1}}
                    >
                        Back
                    </Button>
                    }
                    <Box sx={{ flex: '1 1 auto' }} />
                    {isStepOptional(activeStep) && (
                        <Button color="inherit" onClick={handleSkip} sx={{ mr: 1 }} disabled={isStepInProgress || isStepCompleted(activeStep)}>
                            Skip
                        </Button>
                    )}
                    {shouldShowNextBeforeCompletion(activeStep) &&
                        <Button onClick={handleNext} disabled={!isStepCompleted(activeStep)}>
                            {activeStep === steps.length - 1 ? 'Finish' : 'Next'}
                        </Button>
                    }
                </Box>
            </>
        </Box>
    );
}
