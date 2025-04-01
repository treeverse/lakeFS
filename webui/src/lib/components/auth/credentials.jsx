import React, {useContext, useState} from "react";

import Modal from "react-bootstrap/Modal";
import Table from "react-bootstrap/Table";
import Alert from "react-bootstrap/Alert";
import {DownloadIcon} from "@primer/octicons-react";

import {auth} from "../../api";
import {useAPIWithPagination} from "../../hooks/api";
import {ClipboardButton, DataTable, AlertError, FormattedDate, Loading} from "../controls";
import {ConfirmationButton} from "../modals";
import {Paginator} from "../pagination";
import {AppContext} from "../../hooks/appContext";

export const DownloadCredentialsButton = ({accessKeyId, secretAccessKey, apiEndpoint = '/api/v1'}) => {
    const downloadContent = 'data:application/octet-stream,' + encodeURIComponent(`# lakectl command line configuration - save under the filename $HOME/.lakectl.yaml
credentials:
    access_key_id: ${accessKeyId}
    secret_access_key: ${secretAccessKey}
server:
    endpoint_url: ${window.location.protocol}//${window.location.host}${apiEndpoint}
`);

    return (
        <a className="btn p-2 pl-3 pr-3"
           style={{backgroundColor: '#808080', color: 'white'}}
           href={downloadContent}
           target="_blank" rel="noreferrer" download="lakectl.yaml">
            <DownloadIcon/> Download Credentials
        </a>
    );
};

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
                                msg={
                                    <span>Are you sure you{'\''}d like to delete access key <code>{row.access_key_id}</code>?</span>}
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


export const CredentialsShowModal = ({credentials, show, onHide}) => {
    if (!credentials) return <></>;

    const {state} = useContext(AppContext);
    const buttonVariant = state.settings.darkMode ? "outline-light" : "outline-dark";

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
                            <ClipboardButton variant={buttonVariant} tooltip="Copy to clipboard"
                                             text={credentials.access_key_id}/>
                        </td>
                    </tr>
                    <tr>
                        <td><strong>Secret Access Key</strong></td>
                        <td><code>{credentials.secret_access_key}</code></td>
                        <td>
                            <ClipboardButton variant={buttonVariant} tooltip="Copy to clipboard"
                                             text={credentials.secret_access_key}/>
                        </td>
                    </tr>
                    </tbody>
                </Table>

                <Alert variant="warning" className="mt-3">
                    Copy the Secret Access Key and store it somewhere safe. You will not be able to access it again.
                    <div className="mt-3 text-md-center">
                        <DownloadCredentialsButton
                            accessKeyId={credentials.access_key_id}
                            secretAccessKey={credentials.secret_access_key}
                        />
                    </div>
                </Alert>
            </Modal.Body>
        </Modal>
    );
};
