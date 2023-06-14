import React, { FC } from "react";
import Accordion from "react-bootstrap/Accordion";

import { useRefs } from "../../hooks/repo";
import "../../../styles/checklist.scss";
import RepoOnboardingChecklistItem from "./repoOnboardingChecklistItem";
import { OnboardingStep } from "../../onboarding/types";

export interface StepsWithStatus extends OnboardingStep {
  isCompletedValue: boolean;
  isLoading: boolean;
}

interface RepoOnboardingChecklistProps {
  steps: Array<StepsWithStatus>;
}

const RepoOnboardingChecklist: FC<RepoOnboardingChecklistProps> = ({
  steps,
}) => {
  const { repo } = useRefs();

  if (!repo) {
    return null;
  }

  return (
    <div className="checklist">
      <h5 className="checklist-title">Welcome to lakeFS!</h5>
      <span className="checklist-description">
        Get the most out of lakeFS with the steps below:
      </span>
      <Accordion flush>
        {steps.map((step) =>
          step.showStep() ? (
            <RepoOnboardingChecklistItem
              step={step}
              isCompleted={step.isCompletedValue}
              isLoading={step.isLoading}
              key={step.id}
            />
          ) : null
        )}
      </Accordion>
    </div>
  );
};

export default RepoOnboardingChecklist;
