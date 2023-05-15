import React, { FC } from "react";
import Button from "react-bootstrap/Button";

interface RepoOnboardingCompleteProps {
  dismissChecklist: () => void;
}

const RepoOnboardingComplete: FC<RepoOnboardingCompleteProps> = ({
  dismissChecklist,
}) => (
  <div>
    <h5 className="checklist-title">{`You've done it!`}</h5>
    <Button onClick={dismissChecklist} variant="success">
      Dismiss Checklist
    </Button>
  </div>
);

export default RepoOnboardingComplete;
