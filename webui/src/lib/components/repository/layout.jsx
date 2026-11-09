import React, { useEffect, useState } from 'react';

import Container from 'react-bootstrap/Container';
import Breadcrumb from 'react-bootstrap/Breadcrumb';
import Stack from 'react-bootstrap/Stack';

import { useRefs } from '../../hooks/repo';
import { Outlet, useParams } from 'react-router-dom';
import { RepositoryNavTabs } from './tabs';
import { Link } from '../nav';
import { RefContextProvider } from '../../hooks/repo';
import { ReadOnlyBadge } from '../badges';
import { RepositoryPageTitle } from '../pageTitle';

const RepoNav = () => {
    const { repo, reference } = useRefs();
    const [repoId, setRepoId] = useState('');
    useEffect(() => {
        if (repo) {
            setRepoId(repo.id);
        }
    }, [repo]);

    const repoLink = {
        pathname: '/repositories/:repoId/objects',
        params: { repoId },
    };
    if (reference?.id) {
        repoLink.query = { ref: reference.id };
    }

    return (
        <Stack direction="horizontal" gap={2}>
            <Breadcrumb>
                <Link href={{ pathname: '/repositories' }} component={Breadcrumb.Item}>
                    Repositories
                </Link>
                <Link href={repoLink} component={Breadcrumb.Item}>
                    {repoId}
                </Link>
            </Breadcrumb>
            <ReadOnlyBadge readOnly={repo?.read_only} style={{ marginBottom: 16 }} />
        </Stack>
    );
};

export const RepositoryPageLayout = ({ fluid = 'sm' }) => {
    const [activePage, setActivePage] = useState('objects');
    const { repoId } = useParams();
    return (
        <RefContextProvider>
            <RepositoryTitle repoId={repoId} />
            <div>
                <RepoNav />

                <div className="full-width-tabs-border">
                    <RepositoryNavTabs active={activePage} />
                </div>

                <Container fluid={fluid}>
                    <div className="mt-4">
                        <Outlet context={[setActivePage]} />
                    </div>
                </Container>
            </div>
        </RefContextProvider>
    );
};

const RepositoryTitle = ({ repoId }) => {
    const { repo, reference } = useRefs();
    return (
        <RepositoryPageTitle
            repo={repo?.id === repoId ? repo : null}
            reference={repo?.id === repoId ? reference : null}
            repoId={repoId}
        />
    );
};
