import {AuthLayout} from "../../lib/components/auth/layout";
import {ActionGroup, ActionsBar, Loading, Error, TooltipButton, ClipboardButton} from "../../lib/components/controls";
import {ConfirmationButton} from "../../lib/components/modals";
import {SyncIcon} from "@primer/octicons-react";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import {BreadcrumbItem} from "react-bootstrap";
import useUser from "../../lib/hooks/user";
import Table from "react-bootstrap/Table";
import {useAPIWithPagination} from "../../rest/hooks";
import {auth} from "../../rest/api";
import {useState} from "react";
import {useRouter} from "next/router";
import moment from "moment";
import Button from "react-bootstrap/Button";
import {Paginator} from "../../lib/components/pagination";
import Modal from "react-bootstrap/Modal";
import Alert from "react-bootstrap/Alert";


const CredentialsTable = ({user, refresh, after, onPaginate}) => {
    const [internalRefresh, setInternalRefresh] = useState(false)
    const [revokeError, setRevokeError] = useState(null)

    const {results, error, loading, nextPage} = useAPIWithPagination(() => {
        return auth.listCredentials(user.id, after)
    }, [refresh, internalRefresh, user.id, after])

    if (!!error) return <Error error={error}/>
    if (!!revokeError) return <Error error={revokeError}/>
    if (loading) return <Loading/>

    return (
        <>
            <Table>
                <thead>
                    <tr>
                        <th>Access Key ID</th>
                        <th>Issued At</th>
                        <th/>
                    </tr>
                </thead>
                <tbody>
                {results.map(row => (
                    <tr key={row.access_key_id}>
                        <td>
                            <code>{row.access_key_id}</code>
                            {(user.accessKeyId === row.access_key_id) && <strong>{' '}(current)</strong>}
                        </td>
                        <td>{moment.unix(row.creation_date).format()}</td>
                        <td>
                            <span className="row-hover">
                                {(user.accessKeyId !== row.access_key_id) && <ConfirmationButton
                                    variant="outline-danger"
                                    size="sm"
                                    msg={<span>Are you sure you'd like to delete access key <code>{row.access_key_id}</code>?</span>}
                                    onConfirm={() => {
                                        auth
                                            .deleteCredentials(user.id, row.access_key_id)
                                            .catch(err => {
                                                setRevokeError(err)
                                            })
                                            .then(() => {
                                                setInternalRefresh(!internalRefresh)
                                            })
                                    }}
                                >
                                    Revoke
                                </ConfirmationButton>}
                            </span>
                        </td>
                    </tr>
                ))}
                </tbody>
            </Table>

            <Paginator onPaginate={onPaginate} after={after} nextPage={nextPage}/>
        </>
    )
}

const CredentialsShowModal = ({ credentials, show, onHide }) => {
    if (!credentials) return <></>

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
    )
}


const CredentialsContainer = () => {
    const router = useRouter()
    const { user } = useUser()
    const userId = (!!user) ? user.id : ""
    const [refreshToken, setRefreshToken] = useState(false)
    const [createError, setCreateError] = useState(null)
    const [createdKey, setCreatedKey] = useState(null)
    const { after } = router.query

    const createKey = () => {
        return auth.createCredentials(user.id)
            .catch(err => {
                setCreateError(err)
            }).then(key => {
                setCreateError(null)
                setRefreshToken(!refreshToken)
                return key
            })
    }

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <ConfirmationButton
                        variant="success"
                        modalVariant="success"
                        msg={<span>Create a new Access Key for user <strong>{userId}</strong>?</span>}
                        onConfirm={hide => {
                            createKey()
                                .then(key => {
                                    setCreatedKey(key)
                                }).finally(() => {
                                    hide()
                                })
                        }}>
                        Create Access Key
                    </ConfirmationButton>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <TooltipButton tooltip="Refresh" variant="outline-dark" size="md" onClick={() => { setRefreshToken(!refreshToken) }}>
                        <SyncIcon/>
                    </TooltipButton>
                </ActionGroup>
            </ActionsBar>

            {(!!createError) && <Error error={createError}/>}

            <CredentialsShowModal
                credentials={createdKey}
                show={(!!createdKey)}
                onHide={() => { setCreatedKey(null) }}
            />

            {(!!user) && <CredentialsTable
                user={user}
                refresh={refreshToken}
                after={(!!after) ? after : ""}
                onPaginate={after => router.push({
                    pathname: '/auth/credentials',
                    query: {after}
                })}
            />}
        </>
    )
}

const CredentialsPage = () => {
    return (
        <AuthLayout activeTab="credentials">
            <CredentialsContainer/>
        </AuthLayout>
    )
}


export default CredentialsPage