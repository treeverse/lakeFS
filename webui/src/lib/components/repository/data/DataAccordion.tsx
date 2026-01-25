import React from 'react';
import Accordion from 'react-bootstrap/Accordion';
import Badge from 'react-bootstrap/Badge';
import { TableIcon, FileDirectoryIcon } from '@primer/octicons-react';
import { FaVectorSquare } from 'react-icons/fa';

import { ObjectsTree } from './ObjectsTree';

interface DataAccordionProps {
    repo: { id: string; read_only?: boolean };
    reference: { id: string; type: string };
    config: {
        pre_sign_support?: boolean;
        pre_sign_support_ui?: boolean;
    };
    onNavigate?: (path: string) => void;
    initialPath?: string;
}

export const DataAccordion: React.FC<DataAccordionProps> = ({ repo, reference, config, onNavigate, initialPath }) => {
    return (
        <Accordion defaultActiveKey="objects" className="data-accordion">
            <Accordion.Item eventKey="tables">
                <Accordion.Header className="disabled-accordion-header">
                    <TableIcon className="me-2" />
                    Tables
                    <Badge bg="secondary" className="ms-2">
                        Coming Soon
                    </Badge>
                </Accordion.Header>
                <Accordion.Body className="text-muted disabled-accordion-body">
                    Table management is available in lakeFS Enterprise.
                </Accordion.Body>
            </Accordion.Item>

            <Accordion.Item eventKey="vectors">
                <Accordion.Header className="disabled-accordion-header">
                    <FaVectorSquare className="me-2" />
                    Vectors
                    <Badge bg="secondary" className="ms-2">
                        Coming Soon
                    </Badge>
                </Accordion.Header>
                <Accordion.Body className="text-muted disabled-accordion-body">
                    Vector storage is available in lakeFS Enterprise.
                </Accordion.Body>
            </Accordion.Item>

            <Accordion.Item eventKey="objects">
                <Accordion.Header>
                    <FileDirectoryIcon className="me-2" />
                    Objects
                </Accordion.Header>
                <Accordion.Body className="objects-accordion-body p-0">
                    <ObjectsTree
                        repo={repo}
                        reference={reference}
                        config={config}
                        onNavigate={onNavigate}
                        initialPath={initialPath}
                    />
                </Accordion.Body>
            </Accordion.Item>
        </Accordion>
    );
};
