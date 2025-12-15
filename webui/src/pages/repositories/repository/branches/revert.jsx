import React, {useEffect, useState} from "react";
import { useOutletContext } from "react-router-dom";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import ListGroup from "react-bootstrap/ListGroup";
import Spinner from "react-bootstrap/Spinner";

import {commits as commitsAPI, branches as branchesAPI} from "../../../../lib/api";
import {AlertError, Loading} from "../../../../lib/components/controls";
import {ConfirmationModal} from "../../../../lib/components/modals";
import {MetadataFields} from "../../../../lib/components/repository/metadata";
import {getMetadataIfValid, touchInvalidFields} from "../../../../lib/components/repository/metadataHelpers";
import {useRefs} from "../../../../lib/hooks/repo";
import {useRouter} from "../../../../lib/hooks/router";
import {RepoError} from "../error";


const RevertPreviewPage = () => {
    const router = useRouter();
    const { commits: commitsParam } = router.query;
    const { branchId } = router.params;
    const { repo, loading: refsLoading, error: refsError } = useRefs();

    const [commitDetails, setCommitDetails] = useState([]);
    const [commitsLoading, setCommitsLoading] = useState(true);
    const [commitsError, setCommitsError] = useState(null);
    const [commitMessage, setCommitMessage] = useState('');
    const [allowEmpty, setAllowEmpty] = useState(false);
    const [metadataFields, setMetadataFields] = useState([]);
    const [reverting, setReverting] = useState(false);
    const [revertError, setRevertError] = useState(null);
    const [showConfirmModal, setShowConfirmModal] = useState(false);

    // Fetch commit details
    useEffect(() => {
        if (!repo || !commitsParam) return;

        const fetchCommits = async () => {
            try {
                setCommitsLoading(true);
                const commitIds = commitsParam.split(',');
                const details = await Promise.all(
                    commitIds.map(id => commitsAPI.get(repo.id, id))
                );
                setCommitDetails(details);

                // Generate default commit message
                if (commitIds.length === 1) {
                    setCommitMessage(`Revert commit ${commitIds[0].substr(0, 12)}`);
                } else {
                    const shortIds = commitIds.map(id => id.substr(0, 12)).join(', ');
                    setCommitMessage(`Revert commits ${shortIds}`);
                }

                setCommitsLoading(false);
            } catch (error) {
                setCommitsError(error);
                setCommitsLoading(false);
            }
        };

        fetchCommits();
    }, [repo, commitsParam]);

    const handleApplyClick = () => {
        // Validate metadata before showing confirmation modal
        const metadata = getMetadataIfValid(metadataFields);
        if (!metadata) {
            setMetadataFields(touchInvalidFields(metadataFields));
            return;
        }

        // Validation passed, show confirmation modal
        setShowConfirmModal(true);
    };

    const handleRevert = async () => {
        // Get validated metadata (we already validated before opening modal)
        const metadata = getMetadataIfValid(metadataFields);

        setReverting(true);
        setRevertError(null);
        setShowConfirmModal(false);

        try {
            const commitIds = commitsParam.split(',');

            // Sequential revert calls - same as lakectl branch_revert.go
            for (const commitId of commitIds) {
                await branchesAPI.revert(
                    repo.id,
                    branchId,
                    commitId,
                    0,          // parentNumber
                    allowEmpty, // allowEmpty
                    commitMessage,  // message
                    metadata    // metadata
                );
            }

            // Success - redirect to commits page
            router.push({
                pathname: '/repositories/:repoId/commits',
                params: { repoId: repo.id },
                query: { ref: branchId }
            });
        } catch (error) {
            setRevertError(error);
            setReverting(false);
        }
    };

    const handleCancel = () => {
        router.push({
            pathname: '/repositories/:repoId/commits',
            params: { repoId: repo.id },
            query: { ref: branchId }
        });
    };

    if (refsLoading || commitsLoading) return <Loading/>;
    if (refsError) return <RepoError error={refsError}/>;
    if (commitsError) return <AlertError error={commitsError}/>;

    const commitIds = commitsParam.split(',');
    const confirmationMessage = (
        <>
            <p>Are you sure you want to revert <strong>{commitIds.length}</strong> commit{commitIds.length > 1 ? 's' : ''}?</p>
            <p>This will create <strong>{commitIds.length}</strong> new revert commit{commitIds.length > 1 ? 's' : ''} on branch <strong>{branchId}</strong>, reversing the changes from the selected commits in order.</p>
        </>
    );

    return (
        <div className="mb-5">
            <h2>Revert Commits</h2>

            {/* Commits to Revert */}
            <Card className="mb-3">
                <Card.Header>
                    <strong>Commits to Revert</strong> (in order)
                </Card.Header>
                <ListGroup variant="flush">
                    {commitDetails.map((commit, index) => (
                        <ListGroup.Item key={commit.id}>
                            <div className="d-flex align-items-start">
                                <span className="me-2 text-muted">{index + 1}.</span>
                                <div>
                                    <code>{commit.id.substr(0, 12)}</code>
                                    <span className="ms-2">{commit.message}</span>
                                    <br/>
                                    <small className="text-muted">
                                        by {commit.committer}
                                    </small>
                                </div>
                            </div>
                        </ListGroup.Item>
                    ))}
                </ListGroup>
            </Card>

            {/* Commit Message */}
            <Card className="mb-3">
                <Card.Header>
                    <strong>Revert Commit Details</strong>
                </Card.Header>
                <Card.Body>
                    <Form.Group className="mb-3">
                        <Form.Label>Commit Message</Form.Label>
                        <Form.Control
                            as="textarea"
                            rows={3}
                            value={commitMessage}
                            onChange={(e) => setCommitMessage(e.target.value)}
                            placeholder="Describe the revert"
                        />
                        <Form.Text className="text-muted">
                            Each revert will create a new commit with this message.
                        </Form.Text>
                    </Form.Group>

                    <MetadataFields
                        metadataFields={metadataFields}
                        setMetadataFields={setMetadataFields}
                    />

                    <Form.Group className="mt-3">
                        <Form.Check
                            type="checkbox"
                            id="allow-empty-commit"
                            label="Allow empty commit (revert without changes)"
                            checked={allowEmpty}
                            onChange={(e) => setAllowEmpty(e.target.checked)}
                        />
                        <Form.Text className="text-muted">
                            Check this if the revert produces no changes (e.g., the commit was already reverted).
                        </Form.Text>
                    </Form.Group>
                </Card.Body>
            </Card>

            {/* Changes Preview */}
            <Card className="mb-3">
                <Card.Header>
                    <strong>Changes Preview</strong>
                </Card.Header>
                <Card.Body>
                    <p className="text-muted mb-0">
                        This will reverse the changes introduced by the selected {commitIds.length} commit{commitIds.length > 1 ? 's' : ''}.
                        Each commit will be reverted in the order shown above, creating {commitIds.length} new revert commit{commitIds.length > 1 ? 's' : ''} on the branch.
                    </p>
                </Card.Body>
            </Card>

            {/* Error Display */}
            {revertError && (
                <AlertError error={revertError} className="mb-3"/>
            )}

            {/* Action Buttons */}
            <div className="d-flex justify-content-end gap-2">
                <Button
                    variant="secondary"
                    onClick={handleCancel}
                    disabled={reverting}>
                    Cancel
                </Button>
                <Button
                    variant="danger"
                    onClick={handleApplyClick}
                    disabled={reverting || (!commitMessage.trim() && !allowEmpty)}>
                    {reverting ? (
                        <>
                            <Spinner
                                as="span"
                                animation="border"
                                size="sm"
                                role="status"
                                aria-hidden="true"
                                className="me-2"
                            />
                            Reverting...
                        </>
                    ) : (
                        'Apply'
                    )}
                </Button>
            </div>

            {/* Confirmation Modal */}
            <ConfirmationModal
                show={showConfirmModal}
                variant="danger"
                onConfirm={handleRevert}
                onHide={() => setShowConfirmModal(false)}
                msg={confirmationMessage}
            />
        </div>
    );
};

const RepositoryRevertPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage('commits'), [setActivePage]);
    return <RevertPreviewPage />;
};

export default RepositoryRevertPage;
