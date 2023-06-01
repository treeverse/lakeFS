/* eslint-disable react/jsx-no-target-blank */
import React, { FC } from "react";
import Button from "react-bootstrap/Button";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Container from "react-bootstrap/Container";

interface RepoOnboardingCompleteProps {
  dismissChecklist: () => void;
}

const RepoOnboardingComplete: FC<RepoOnboardingCompleteProps> = ({
  dismissChecklist,
}) => (
  <Container className="complete-card">
    <Row>
      <Col className="d-flex justify-content-center pb-4">
        <img src="/trophy.svg" alt="trophy" width="72" height="72" />
      </Col>
    </Row>
    <Row>
      <Col className="d-flex justify-content-center mb-2">
        <h5 className="checklist-complete-title">{`You've done it!`}</h5>
      </Col>
    </Row>
    <Row className="g-0">
      <Col className="d-flex justify-content-center checklist-complete-text">
        <div>
          {`You're lakeFS repository is now set up for data versioning. You can branch to create isolated versions of your data, commit changes to create reproducible snapshots, and merge to incorporate your changes in one atomic operation. Visit our`}{" "}
          <a href="https://docs.lakefs.io/#welcome-to-the-lake" target="_blank">
            docs
          </a>
          {` for more on using lakeFS and integrating it with your data ecosystem.`}
        </div>
      </Col>
    </Row>
    <Row>
      <Col className="d-flex justify-content-center mt-4">
        <Button
          className="dismiss-button"
          onClick={dismissChecklist}
          variant="link"
        >
          Dismiss
        </Button>
      </Col>
    </Row>
  </Container>
);

export default RepoOnboardingComplete;
