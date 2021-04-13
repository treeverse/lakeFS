import {AuthLayout} from "../../lib/components/auth/layout";
import {
    ActionGroup,
    ActionsBar,
    Loading,
    Error,
    ClipboardButton,
    DataTable, RefreshButton, FormattedDate
} from "../../lib/components/controls";
import {ConfirmationButton} from "../../lib/components/modals";
import useUser from "../../lib/hooks/user";
import {auth} from "../../rest/api";
import {useState} from "react";
import {useRouter} from "next/router";

import {CredentialsShowModal, CredentialsTable} from "../../lib/components/auth/credentials";


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
                                .then(key => { setCreatedKey(key) })
                                .finally(hide)
                        }}>
                        Create Access Key
                    </ConfirmationButton>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <RefreshButton variant="outline-dark" size="md" onClick={() => setRefreshToken(!refreshToken)}/>
                </ActionGroup>
            </ActionsBar>

            {(!!createError) && <Error error={createError}/>}

            <CredentialsShowModal
                credentials={createdKey}
                show={(!!createdKey)}
                onHide={() => { setCreatedKey(null) }}/>

            {(!!user) && <CredentialsTable
                userId={user.id}
                currentAccessKey={user.accessKeyId}
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
