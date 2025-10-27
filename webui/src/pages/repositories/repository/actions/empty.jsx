import React from "react";
import {
    PlayIcon,
    BookIcon,
    CheckCircleIcon,
    LightBulbIcon
} from "@primer/octicons-react";
import {Card, Row, Col, Button} from "react-bootstrap";
import { TextRenderer } from "../fileRenderers/simple";

// Example used for empty state
const yamlExample = `# Actions are configured in the _lakefs_actions/ 
#  directory at the root of your repository.
# Example:
# _lakefs_actions/pre_merge_format_check.yaml
name: pre merge format check on main
on:
  pre-merge:
    branches:
      - main
hooks:
  - id: check_formats
    type: lua
    properties:
      # location of this script in the repository!
      script_path: "scripts/format_validator.lua" 
    args:
        allow_list: ["parquet", "orc", "log"]
        ignore_hidden:  true
`;

export const EmptyActionsState = () => {
    return (
        <div className="mt-4 mb-4">
            <Row>
                {/* Left Column - Why use actions */}
                <Col md={6} className="mb-3">
                    <h3 className="mb-4">
                      <PlayIcon className="me-2" size={32}/>
                      No actions configured yet
                    </h3>
                    <p className="text-muted mb-4">
                        Actions are automated workflows that run when specific events occur in your repository, 
                        like commits or merges. They help you implement <strong>CI/CD for data</strong> by 
                        validating data quality, enforcing policies, and automating data governance.
                    </p>
                    <h6 className="my-4">Why use actions?</h6>
                    <div className="d-flex align-items-start mb-3">
                        <CheckCircleIcon size={16} className="text-success me-2 mt-1" />
                        <div>
                            <strong>Data Quality Gates</strong>
                            <p className="text-muted small mb-0">Automatically validate schema, format, and content before data reaches production</p>
                        </div>
                    </div>
                    <div className="d-flex align-items-start mb-3">
                        <CheckCircleIcon size={16} className="text-success me-2 mt-1" />
                        <div>
                            <strong>Policy Enforcement</strong>
                            <p className="text-muted small mb-0">Ensure compliance with data governance policies and security requirements</p>
                        </div>
                    </div>
                    <div className="d-flex align-items-start mb-3">
                        <CheckCircleIcon size={16} className="text-success me-2 mt-1" />
                        <div>
                            <strong>Automated Workflows</strong>
                            <p className="text-muted small mb-0">Trigger notifications, data processing, or external system updates</p>
                        </div>
                    </div>

                    <div className="d-flex flex-column flex-sm-row  gap-2 mt-4">
                        <Button 
                            href="https://docs.lakefs.io/latest/howto/hooks/" 
                            target="_blank" 
                            rel="noopener noreferrer"
                            variant="success" 
                            size="md"
                            className="px-3 py-2"
                        >
                            <BookIcon className="me-2" />
                            Learn How to Configure Actions
                        </Button>
                        <Button 
                            href="https://docs.lakefs.io/latest/understand/use_cases/cicd_for_data/" 
                            target="_blank" 
                            rel="noopener noreferrer"
                            variant="outline-secondary" 
                            size="md"
                            className="px-3 py-2"
                        >
                            <PlayIcon className="me-2" />
                            CI/CD for Data Guide
                        </Button>
                    </div>
                </Col>

                <Col md={6}>
                    <Card className="shadow-sm" variant="yotub">
                        <Card.Header>
                            <LightBulbIcon className="me-2" size={18}/> 
                            Example: Data Quality Validation
                        </Card.Header>
                        <Card.Body className="p-0">
                          <TextRenderer text={yamlExample} fileExtension="yaml" contentType="text/yaml"/>
                        </Card.Body>
                    </Card>
                </Col>
            </Row>
        </div>
    );
};
