import React, { useEffect, useState, useCallback } from 'react';
import { useOutletContext } from 'react-router-dom';
import Button from 'react-bootstrap/Button';
import Form from 'react-bootstrap/Form';
import Alert from 'react-bootstrap/Alert';

import { ActionGroup, ActionsBar, AlertError, Loading } from '../../../../lib/components/controls';
import { useRefs } from '../../../../lib/hooks/repo';
import { RepoError } from '../error';
import { useRouter } from '../../../../lib/hooks/router';
import { pulls as pullsAPI } from '../../../../lib/api';
import { DataBrowserLayout } from '../../../../lib/components/repository/data';
import CompareBranchesSelection from '../../../../lib/components/repository/compareBranchesSelection';
import { RefTypeBranch } from '../../../../constants';
import { useConfigContext } from '../../../../lib/hooks/configProvider';

const CreatePullForm = ({
    repo,
    reference,
    compare,
    title,
    setTitle,
    description,
    setDescription,
    isEmptyDiff,
    diffError,
}) => {
    const router = useRouter();
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    const onTitleInput = ({ target: { value } }) => setTitle(value);
    const onDescriptionInput = ({ target: { value } }) => setDescription(value);

    const submitForm = async () => {
        setLoading(true);
        setError(null);
        try {
            const { id: createdPullId } = await pullsAPI.create(repo.id, {
                title,
                description,
                source_branch: compare.id,
                destination_branch: reference.id,
            });

            router.push({
                pathname: `/repositories/:repoId/pulls/:pullId`,
                params: { repoId: repo.id, pullId: createdPullId },
            });
        } catch (err) {
            setError(err.message);
            setLoading(false);
        }
    };

    return (
        <>
            <Form.Group className="mb-3">
                <Form.Control
                    required
                    disabled={loading}
                    type="text"
                    size="lg"
                    placeholder="Add a title..."
                    value={title}
                    onChange={onTitleInput}
                />
            </Form.Group>
            <Form.Group className="mb-3">
                <Form.Control
                    required
                    disabled={loading}
                    as="textarea"
                    rows={8}
                    placeholder="Describe your changes..."
                    value={description}
                    onChange={onDescriptionInput}
                />
            </Form.Group>
            {error && <AlertError error={error} onDismiss={() => setError(null)} />}
            {diffError && <AlertError error={diffError} />}
            <div>
                <Button
                    variant="success"
                    disabled={!title || loading || isEmptyDiff || !!diffError}
                    onClick={submitForm}
                >
                    {loading && (
                        <>
                            <span className="spinner-border spinner-border-sm text-light" role="status" />{' '}
                        </>
                    )}
                    Create Pull Request
                </Button>
                {isEmptyDiff && !diffError && (
                    <span className="alert alert-warning align-middle ms-4 pt-2 pb-2">
                        Pull requests must include changes.
                    </span>
                )}
            </div>
        </>
    );
};

const CreatePull = () => {
    const router = useRouter();
    const { repo, loading, error, reference, compare } = useRefs();
    const { config, loading: configLoading, error: configError } = useConfigContext();
    const { prefix } = router.query;

    const [title, setTitle] = useState('');
    const [description, setDescription] = useState('');
    const [isEmptyDiff, setIsEmptyDiff] = useState(false);
    const [diffError, setDiffError] = useState(null);

    const handleNavigate = useCallback(
        (path) => {
            router.push({
                pathname: '/repositories/:repoId/pulls/create',
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

    const handleDiffStatusChange = useCallback((status) => {
        if (!status.loading) {
            setIsEmptyDiff(status.isEmpty);
            setDiffError(status.error);
        }
    }, []);

    if (loading || configLoading) return <Loading />;
    if (error) return <RepoError error={error} />;
    if (configError) return <RepoError error={configError} />;

    // Nothing to compare if refs are the same
    if (compare.id === reference.id) {
        return (
            <div className="create-pull-page">
                <ActionsBar>
                    <ActionGroup orientation="left">
                        <CompareBranchesSelection
                            repo={repo}
                            reference={reference}
                            compareReference={compare}
                            baseSelectURL={'/repositories/:repoId/pulls/create'}
                        />
                    </ActionGroup>
                </ActionsBar>
                <h1 className="mt-3">Create Pull Request</h1>
                <Alert variant="warning" className="mt-4">
                    <Alert.Heading>There is nothing to compare.</Alert.Heading>
                    You need to use two different branches to create a pull request.
                </Alert>
            </div>
        );
    }

    // Create diff mode configuration: compare reference (base/destination) to compare (source)
    const diffModeConfig = {
        enabled: true,
        leftRef: reference.id,
        rightRef: compare.id,
    };

    return (
        <div className="create-pull-page">
            <ActionsBar>
                <ActionGroup orientation="left">
                    <CompareBranchesSelection
                        repo={repo}
                        reference={reference}
                        compareReference={compare}
                        baseSelectURL={'/repositories/:repoId/pulls/create'}
                    />
                </ActionGroup>
            </ActionsBar>
            <h1 className="mt-3">Create Pull Request</h1>
            <div className="mt-4">
                <CreatePullForm
                    repo={repo}
                    reference={reference}
                    compare={compare}
                    title={title}
                    setTitle={setTitle}
                    description={description}
                    setDescription={setDescription}
                    isEmptyDiff={isEmptyDiff}
                    diffError={diffError}
                />
            </div>
            <hr className="mt-5 mb-4" />
            <div className="create-pull-diff-browser">
                <DataBrowserLayout
                    repo={repo}
                    reference={{ id: reference.id, type: RefTypeBranch }}
                    config={config || {}}
                    initialPath={prefix}
                    onNavigate={handleNavigate}
                    diffMode={diffModeConfig}
                    showOnlyChanges={true}
                    onDiffStatusChange={handleDiffStatusChange}
                />
            </div>
        </div>
    );
};

const RepositoryCreatePullPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage('pulls'), [setActivePage]);
    return <CreatePull />;
};

export default RepositoryCreatePullPage;
