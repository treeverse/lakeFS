import React, { useEffect } from 'react';
import { useLocation } from 'react-router-dom';
import { RefTypeBranch } from '../../constants';

export const PageTitle = ({ title }) => {
    useEffect(() => {
        document.title = title ? `${title} | lakeFS` : 'lakeFS';
    }, [title]);
    return null;
};

export const RepositoryPageTitle = ({ repo, reference, repoId }) => {
    const { pathname } = useLocation();
    const repositoryName = repo?.id || repoId;
    const repositoryPath = `/repositories/${encodeURIComponent(repoId)}`;
    let title = repositoryName;

    if (pathname === `${repositoryPath}/pulls`) {
        title = `Pull Requests - ${repositoryName}`;
    } else if (pathname === `${repositoryPath}/pulls/create`) {
        title = `Create Pull Request - ${repositoryName}`;
    } else if (pathname.startsWith(`${repositoryPath}/pulls/`)) {
        title = `Pull Request - ${repositoryName}`;
    } else if (
        (pathname === `${repositoryPath}/objects` || pathname === `${repositoryPath}/object`) &&
        reference?.type === RefTypeBranch
    ) {
        title = `${repositoryName} / ${reference.id}`;
    }

    return <PageTitle title={title} />;
};
