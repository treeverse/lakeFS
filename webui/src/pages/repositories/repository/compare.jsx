import React, { useEffect, useState, useCallback } from 'react';
import { useOutletContext } from 'react-router-dom';
import Alert from 'react-bootstrap/Alert';
import { Loading } from '../../../lib/components/controls';
import { useRefs } from '../../../lib/hooks/repo';
import { useRouter } from '../../../lib/hooks/router';
import { RepoError } from './error';
import { DataBrowserLayout } from '../../../lib/components/repository/data';
import CompareBranchesActionsBar from '../../../lib/components/repository/compareBranchesActionBar';
import { useConfigContext } from '../../../lib/hooks/configProvider';

const CompareContainer = () => {
    const router = useRouter();
    const { loading, error, repo, reference, compare } = useRefs();
    const { config, loading: configLoading, error: configError } = useConfigContext();

    const { prefix } = router.query;
    const [refreshToken, setRefreshToken] = useState(false);
    const [isEmptyDiff, setIsEmptyDiff] = useState(false);

    const handleNavigate = useCallback(
        (path) => {
            router.push({
                pathname: '/repositories/:repoId/compare',
                params: { repoId: repo.id },
                query: {
                    ref: reference.id,
                    compare: compare.id,
                    prefix: path,
                },
            });
        },
        [router, repo?.id, reference?.id, compare?.id],
    );

    const handleRefresh = useCallback(() => {
        setRefreshToken((prev) => !prev);
    }, []);

    const handleDiffStatusChange = useCallback((status) => {
        if (!status.loading) {
            setIsEmptyDiff(status.isEmpty);
        }
    }, []);

    if (loading || configLoading) return <Loading />;
    if (error) return <RepoError error={error} />;
    if (configError) return <RepoError error={configError} />;

    // Nothing to compare if refs are the same
    if (compare.id === reference.id) {
        return (
            <>
                <CompareBranchesActionsBar
                    repo={repo}
                    reference={reference}
                    compareReference={compare}
                    baseSelectURL="/repositories/:repoId/compare"
                    doRefresh={handleRefresh}
                    isEmptyDiff={true}
                />
                <Alert variant="warning">
                    <Alert.Heading>There is nothing to compare.</Alert.Heading>
                    You need to use two different sources to get a valid comparison.
                </Alert>
            </>
        );
    }

    // Create diff mode configuration: compare reference (base) to compare (target)
    const diffModeConfig = {
        enabled: true,
        leftRef: reference.id,
        rightRef: compare.id,
    };

    return (
        <>
            <CompareBranchesActionsBar
                repo={repo}
                reference={reference}
                compareReference={compare}
                baseSelectURL="/repositories/:repoId/compare"
                doRefresh={handleRefresh}
                isEmptyDiff={isEmptyDiff}
            />
            <div className="mt-3 compare-diff-browser">
                <DataBrowserLayout
                    repo={repo}
                    reference={reference}
                    config={config || {}}
                    initialPath={prefix}
                    onNavigate={handleNavigate}
                    refreshToken={refreshToken}
                    diffMode={diffModeConfig}
                    showOnlyChanges={true}
                    onDiffStatusChange={handleDiffStatusChange}
                />
            </div>
        </>
    );
};

const RepositoryComparePage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage('compare'), [setActivePage]);
    return <CompareContainer />;
};

export default RepositoryComparePage;
