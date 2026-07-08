import React from 'react';

import Nav from 'react-bootstrap/Nav';
import { WorkflowIcon, GitPullRequestIcon, GearIcon } from '@primer/octicons-react';

import { Link, NavItem } from '../nav';
import { useRouter } from '../../hooks/router';

export const DatasetsNavTabs = () => {
    const { route } = useRouter();
    const isActive = (suffix) =>
        suffix === '' ? route === '/datasets' || route === '/datasets/' : route.startsWith(`/datasets/${suffix}`);

    return (
        <Nav variant="tabs">
            <Link active={isActive('')} href="/datasets" component={NavItem}>
                <WorkflowIcon /> Datasets
            </Link>
            <Link active={isActive('pulls')} href="/datasets/pulls" component={NavItem}>
                <GitPullRequestIcon /> Pull Requests
            </Link>
            <Link active={isActive('settings')} href="/datasets/settings" component={NavItem}>
                <GearIcon /> Settings
            </Link>
        </Nav>
    );
};
