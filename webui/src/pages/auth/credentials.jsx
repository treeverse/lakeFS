import React from "react";

import {AuthLayout} from "../../lib/components/auth/layout";
import {
    ActionGroup,
    ActionsBar,
    AlertError,
    RefreshButton
} from "../../lib/components/controls";
import {ConfirmationButton} from "../../lib/components/modals";
import useUser from "../../lib/hooks/user";
import {auth} from "../../lib/api";
import {useState} from "react";
import {CredentialsShowModal, CredentialsTable} from "../../lib/components/auth/credentials";
import {useRouter} from "../../lib/hooks/router";


const CredentialsContainer = () => {
    const router = useRouter();
    const { user } = useUser();
    const userId = (user) ? user.id : "";
    const [refreshToken, setRefreshToken] = useState(false);
    const [createError, setCreateError] = useState(null);
    const [createdKey, setCreatedKey] = useState(null);
    const { after } = router.query;

    const createKey = () => {
        return auth.createCredentials(user.id)
            .catch(err => {
                setCreateError(err);
            }).then(key => {
                setCreateError(null);
                setRefreshToken(!refreshToken);
                return key;
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
                                .finally(hide);
                        }}>
                        Create Access Key
                    </ConfirmationButton>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefreshToken(!refreshToken)}/>
                </ActionGroup>
            </ActionsBar>
            <div className="auth-learn-more">
                An access key-pair is the set of credentials used to access lakeFS. <a href="https://docs.lakefs.io/reference/authorization.html#authentication" target="_blank" rel="noopener noreferrer">Learn more.</a>
            </div>

            {(!!createError) && <AlertError error={createError}/>}

            <CredentialsShowModal
                credentials={createdKey}
                show={(!!createdKey)}
                onHide={() => { setCreatedKey(null) }}/>

            {(!!user) && <CredentialsTable
                userId={user.id}
                currentAccessKey={user.accessKeyId}
                refresh={refreshToken}
                after={(after) ? after : ""}
                onPaginate={after => router.push({
                    pathname: '/auth/credentials',
                    query: {after}
                })}
            />}
        </>
    );
};

const CredentialsPage = () => {
    return (
        <AuthLayout activeTab="credentials">
            <CredentialsContainer/>
        </AuthLayout>
    );
};


export default CredentialsPage;
