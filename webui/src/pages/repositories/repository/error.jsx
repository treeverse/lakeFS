import {useRouter} from "../../../lib/hooks/router";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import {repositories, RepositoryDeletionError, AuthenticationError} from "../../../lib/api";
import {TrashIcon} from "@primer/octicons-react";
import React from "react";
import {AlertError} from "../../../lib/components/controls";

const RepositoryInDeletionContainer = ({repoId}) => {
    const router = useRouter();
    return (
        <Alert variant="warning">
            <Alert.Heading>Repository is undergoing deletion</Alert.Heading>
            This may take several seconds. You can retry the deletion process by pressing the delete button again.
            <hr />
            <div className="d-flex justify-content-end">
                <Button variant="danger" className="mt-3" onClick={
                    async () => {
                        try {
                            await repositories.delete(repoId);
                        } catch {
                            // continue regardless of error
                        }
                        return router.push('/repositories')
                    }}>
                    <TrashIcon/> Delete Repository
                </Button>
            </div>
        </Alert>
    );
};

export const RepoError = ({error, router}) => {
    if (error instanceof AuthenticationError) {
        router.push({
            pathname: '/auth/login',
            query: {next: router.route, redirected: true},
        });
        return null;
    }
    
    if (error instanceof RepositoryDeletionError) {
        return <RepositoryInDeletionContainer repoId={error.repoId}/>;
    }
    return <AlertError error={error}/>;
};
