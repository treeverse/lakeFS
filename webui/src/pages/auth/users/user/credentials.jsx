import React, {useState} from "react";

import {AuthLayout} from "../../../../lib/components/auth/layout";
import {UserHeaderWithContext} from "./userHeaderWithContext";
import {auth} from "../../../../lib/api";
import {CredentialsShowModal, CredentialsTable} from "../../../../lib/components/auth/credentials";
import useUser from "../../../../lib/hooks/user";
import {ConfirmationButton} from "../../../../lib/components/modals";
import {
    ActionGroup,
    ActionsBar,
    Error,
    RefreshButton
} from "../../../../lib/components/controls";
import {useRouter} from "../../../../lib/hooks/router";


const UserCredentialsList = ({ userId, after, onPaginate }) => {
    const {user} = useUser();
    const [refresh, setRefresh] = useState(false);
    const [createError, setCreateError] = useState(null);
    const [createdKey, setCreatedKey] = useState(null);

    const createKey = () => {
        return auth.createCredentials(userId)
            .catch(err => {
                setCreateError(err);
            }).then(key => {
                setCreateError(null);
                setRefresh(!refresh);
                return key;
            });
    };
    const content = (
            <>
                {createError && <Error error={createError}/>}
                <CredentialsTable
                    userId={userId}
                    currentAccessKey={(user) ? user.accessKeyId : ""}
                    refresh={refresh}
                    after={after}
                    onPaginate={onPaginate}
                />
            </>
        );

    return (
        <>
            <UserHeaderWithContext userId={userId} page={'credentials'}/>

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
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>

            <div className="mt-2">

                <CredentialsShowModal
                    credentials={createdKey}
                    show={(!!createdKey)}
                    onHide={() => { setCreatedKey(null) }}/>

                {content}
            </div>
        </>
    );
}

const UserCredentialsContainer = () => {
    const router = useRouter();
    const { after } = router.query;
    const { userId } = router.params;
    return (!userId) ? <></> : <UserCredentialsList
        userId={userId}
        after={(after) ? after : ""}
        onPaginate={after => router.push({pathname: '/auth/users/:userId/credentials', query: {after}, params: {userId}})}
    />;
};

const UserCredentialsPage = () => {
    return (
        <AuthLayout activeTab="users">
            <UserCredentialsContainer/>
        </AuthLayout>
    );
};

export default UserCredentialsPage;