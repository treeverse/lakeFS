import React, { FC } from 'react';

import Nav from 'react-bootstrap/Nav';
import { GitPullRequestIcon, GearIcon } from '@primer/octicons-react';
import { FaCubesStacked } from 'react-icons/fa6';

interface DatasetsNavTabsProps {
    active: string;
    onSelect: (eventKey: string | null) => void;
}

export const DatasetsNavTabs: FC<DatasetsNavTabsProps> = ({ active, onSelect }) => {
    return (
        <Nav variant="tabs" className="datasets-nav-tabs" activeKey={active} onSelect={onSelect}>
            <Nav.Item>
                <Nav.Link eventKey="datasets">
                    <FaCubesStacked /> Datasets
                </Nav.Link>
            </Nav.Item>
            <Nav.Item>
                <Nav.Link eventKey="pulls">
                    <GitPullRequestIcon /> Pull Requests
                </Nav.Link>
            </Nav.Item>
            <Nav.Item>
                <Nav.Link eventKey="settings">
                    <GearIcon /> Settings
                </Nav.Link>
            </Nav.Item>
        </Nav>
    );
};
