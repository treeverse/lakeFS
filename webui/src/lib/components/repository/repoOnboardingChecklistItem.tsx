import React, { FC } from "react";
import { OnboardingStep } from "../../onboarding/types";
import Accordion from "react-bootstrap/Accordion";
import Button from "react-bootstrap/Button";
import { BsCheckCircleFill, BsCircle, BsArrowRightShort } from "react-icons/bs";
import { CircularProgress } from "@mui/material";

interface RepoOnboardingChecklistItemProps {
  step: OnboardingStep;
  isCompleted: boolean;
  isLoading: boolean;
}

const RepoOnboardingChecklistItem: FC<RepoOnboardingChecklistItemProps> = ({
  step,
  isCompleted,
  isLoading,
}) => {
  let iconComponent = <CircularProgress size={32} color="success" />;
  if (!isLoading) {
    if (isCompleted) {
      iconComponent = (
        <BsCheckCircleFill size="32" style={{ color: "#198754" }} />
      );
    } else {
      iconComponent = <BsCircle size="32" style={{ color: "#198754" }} />;
    }
  }

  return (
    <Accordion.Item eventKey={step.id} key={step.id}>
      <Accordion.Header className="checklist-item-header">
        {iconComponent}
        <span className="checklist-item-header-text">{step.title}</span>
      </Accordion.Header>
      <Accordion.Body>
        <div className="checklist-item-text">{step.description}</div>
        <Button className="cta-button" variant="link" onClick={step.onClick}>
          <span className="cta-text">{step.cta}</span>
          <BsArrowRightShort size="24" />
        </Button>
      </Accordion.Body>
    </Accordion.Item>
  );
};

export default RepoOnboardingChecklistItem;
