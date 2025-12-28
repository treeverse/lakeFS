import { useRouter } from '../../../lib/hooks/router';
import Alert from 'react-bootstrap/Alert';
import Button from 'react-bootstrap/Button';
import { repositories, RepositoryDeletionError, BareRepositoryError } from '../../../lib/api';
import { TrashIcon } from '@primer/octicons-react';
import React from 'react';
import { AlertError } from '../../../lib/components/controls';

const RepositoryInDeletionContainer = ({ repoId }) => {
    const router = useRouter();
    return (
        <Alert variant="warning">
            <Alert.Heading>Repository is undergoing deletion</Alert.Heading>
            This may take several seconds. You can retry the deletion process by pressing the delete button again.
            <hr />
            <div className="d-flex justify-content-end">
                <Button
                    variant="danger"
                    className="mt-3"
                    onClick={async () => {
                        try {
                            await repositories.delete(repoId);
                        } catch {
                            // continue regardless of error
                        }
                        return router.push('/repositories');
                    }}
                >
                    <TrashIcon /> Delete Repository
                </Button>
            </div>
        </Alert>
    );
};

const BareRepositoryContainer = () => (
    <Alert variant="info">
        <Alert.Heading>Repository Not Initialized</Alert.Heading>
        <p className="mb-2">
            This repository is empty (bare) and has no branches or commits. Bare repositories are typically used for
            backup/restore operations with <code>lakectl refs-restore</code>.
        </p>
        <p>
            <a href="https://docs.lakefs.io/howto/backup-and-restore.html" target="_blank" rel="noopener noreferrer">
                Learn more about bare repositories.
            </a>
        </p>
    </Alert>
);

export const RepoError = ({ error }) => {
    if (error instanceof RepositoryDeletionError) {
        return <RepositoryInDeletionContainer repoId={error.repoId} />;
    }
    if (error instanceof BareRepositoryError) {
        return <BareRepositoryContainer />;
    }
    return <AlertError error={error} />;
};
