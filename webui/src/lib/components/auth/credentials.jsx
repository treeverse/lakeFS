import React, {useState} from "react";

import Modal from "react-bootstrap/Modal";
import Table from "react-bootstrap/Table";
import Alert from "react-bootstrap/Alert";

import {auth} from "../../api";
import {useAPIWithPagination} from "../../hooks/api";
import {ClipboardButton, DataTable, AlertError, FormattedDate, Loading} from "../controls";
import {ConfirmationButton} from "../modals";
import {Paginator} from "../pagination";


export const CredentialsTable = ({userId, currentAccessKey, refresh, after, onPaginate}) => {
    const [internalRefresh, setInternalRefresh] = useState(false);
    const [revokeError, setRevokeError] = useState(null);

    const {results, error, loading, nextPage} = useAPIWithPagination(() => {
        return auth.listCredentials(userId, after);
    }, [refresh, internalRefresh, userId, after]);

    if (error) return <AlertError error={error}/>;
    if (revokeError) return <AlertError error={revokeError}/>;
    if (loading) return <Loading/>;

    return (
        <>
            <DataTable
                keyFn={row => row.access_key_id}
                emptyState={'No credentials found'}
                results={results}
                headers={['Access Key ID', 'Creation Date', '']}
                rowFn={row => [
                    <>
                        <code>{row.access_key_id}</code>
                        {(currentAccessKey === row.access_key_id) && <strong>{' '}(current)</strong>}
                    </>,
                    <FormattedDate dateValue={row.creation_date}/>,
                    <span className="row-hover">
                    {(currentAccessKey !== row.access_key_id) &&
                    <ConfirmationButton
                        variant="outline-danger"
                        size="sm"
                        msg={<span>Are you sure you{'\''}d like to delete access key <code>{row.access_key_id}</code>?</span>}
                        onConfirm={() => {
                            auth.deleteCredentials(userId, row.access_key_id)
                                .catch(err => setRevokeError(err))
                                .then(() => setInternalRefresh(!internalRefresh))
                        }}>
                        Revoke
                    </ConfirmationButton>
                    }
                    </span>
                ]}
            />
            <Paginator onPaginate={onPaginate} after={after} nextPage={nextPage}/>
        </>
    );
};


export const CredentialsShowModal = ({ credentials, show, onHide }) => {
    if (!credentials) return <></>;

    return (
        <Modal show={show} onHide={onHide} size="lg">
            <Modal.Header closeButton>
                <Modal.Title>Create Access Key</Modal.Title>
            </Modal.Header>

            <Modal.Body>
                <Table>
                    <tbody>
                    <tr>
                        <td><strong>Access Key ID</strong></td>
                        <td><code>{credentials.access_key_id}</code></td>
                        <td>
                            <ClipboardButton variant="outline-dark" tooltip="Copy to clipboard" text={credentials.access_key_id}/>
                        </td>
                    </tr>
                    <tr>
                        <td><strong>Secret Access Key</strong></td>
                        <td><code>{credentials.secret_access_key}</code></td>
                        <td>
                            <ClipboardButton variant="outline-dark" tooltip="Copy to clipboard" text={credentials.secret_access_key}/>
                        </td>
                    </tr>
                    </tbody>
                </Table>

                <Alert variant="warning" className="mt-3">Copy the Secret Access Key and store it somewhere safe. You will not be able to access it again.</Alert>
            </Modal.Body>
        </Modal>
    );
};
