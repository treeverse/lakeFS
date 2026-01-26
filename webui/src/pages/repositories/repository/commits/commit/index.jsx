import React, { useEffect } from 'react';
import Alert from 'react-bootstrap/Alert';
import { AlertError, Loading } from '../../../../../lib/components/controls';
import { useRefs } from '../../../../../lib/hooks/repo';
import { useAPI } from '../../../../../lib/hooks/api';
import { commits } from '../../../../../lib/api';
import { useRouter } from '../../../../../lib/hooks/router';
import { CommitInfoCard } from '../../../../../lib/components/repository/commits';
import { DataBrowserLayout } from '../../../../../lib/components/repository/data';
import { useOutletContext } from 'react-router-dom';
import { useConfigContext } from '../../../../../lib/hooks/configProvider';

const CommitView = ({ repo, commitId, prefix, onNavigate, config }) => {
    const { response, loading, error } = useAPI(async () => {
        return await commits.get(repo.id, commitId);
    }, [repo.id, commitId]);

    if (loading) return <Loading />;
    if (error) return <AlertError error={error} />;

    const commit = response;
    const hasParent = commit.parents && commit.parents.length > 0;

    // For commits without a parent (initial commit), we can't show a diff
    if (!hasParent) {
        return (
            <div className="mb-5 mt-3">
                <CommitInfoCard repo={repo} commit={commit} />
                <div className="mt-4">
                    <Alert variant="info">This is the initial commit. No changes to display.</Alert>
                </div>
            </div>
        );
    }

    // Create a reference object for the commit
    const commitRef = {
        id: commit.id,
        type: 'commit',
    };

    // Diff mode configuration: compare parent to this commit
    const diffModeConfig = {
        enabled: true,
        leftRef: commit.parents[0],
        rightRef: commit.id,
    };

    return (
        <div className="mb-5 mt-3">
            <CommitInfoCard repo={repo} commit={commit} />
            <div className="mt-4 commit-diff-browser">
                <DataBrowserLayout
                    repo={repo}
                    reference={commitRef}
                    config={config || {}}
                    initialPath={prefix}
                    onNavigate={onNavigate}
                    diffMode={diffModeConfig}
                    showOnlyChanges={true}
                />
            </div>
        </div>
    );
};

const CommitContainer = () => {
    const router = useRouter();
    const { repo, loading, error } = useRefs();
    const { config, loading: configLoading, error: configError } = useConfigContext();
    const { prefix } = router.query;
    const { commitId } = router.params;

    if (loading || configLoading) return <Loading />;
    if (error) return <AlertError error={error} />;
    if (configError) return <AlertError error={configError} />;

    const handleNavigate = (path) => {
        router.push({
            pathname: '/repositories/:repoId/commits/:commitId',
            params: { repoId: repo.id, commitId: commitId },
            query: { prefix: path },
        });
    };

    return (
        <CommitView repo={repo} prefix={prefix || ''} commitId={commitId} onNavigate={handleNavigate} config={config} />
    );
};

const RepositoryCommitPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage('commits'), [setActivePage]);

    return <CommitContainer />;
};

export default RepositoryCommitPage;
