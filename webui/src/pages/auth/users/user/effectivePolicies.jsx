import React, {useState} from "react";

import {AuthLayout} from "../../../../lib/components/auth/layout";
import {UserHeaderWithContext} from "./userHeaderWithContext";
import {useAPIWithPagination} from "../../../../lib/hooks/api";
import {auth} from "../../../../lib/api";
import {Paginator} from "../../../../lib/components/pagination";
import {
    ActionGroup,
    ActionsBar,
    DataTable,
    FormattedDate,
    Loading,
    AlertError,
    RefreshButton
} from "../../../../lib/components/controls";
import {Link} from "../../../../lib/components/nav";
import {useRouter} from "../../../../lib/hooks/router";


const UserEffectivePoliciesList = ({ userId, after, onPaginate }) => {

    const [refresh, setRefresh] = useState(false);

    const {results, loading, error, nextPage} = useAPIWithPagination(() => {
        return auth.listUserPolicies(userId, true, after);
    }, [userId, after, refresh]);

    let content;
    if (loading) content = <Loading/>;
    else if (error) content=  <AlertError error={error}/>;
    else content = (
            <>
               <DataTable
                    keyFn={policy => policy.id}
                    rowFn={policy => [
                        <Link href={{pathname: '/auth/policies/:policyId', params: {policyId: policy.id}}}>{policy.id}</Link>,
                        <FormattedDate dateValue={policy.creation_date}/>
                    ]}
                    headers={['Policy ID', 'Created At']}
                    results={results}
                    emptyState={'No policies found'}
                />

                <Paginator onPaginate={onPaginate} after={after} nextPage={nextPage}/>
            </>
        );

    return (
        <>
            <UserHeaderWithContext userId={userId} page={'effectivePolicies'}/>

            <ActionsBar>
                <ActionGroup orientation="left">
                    <p>
                        <small>
                            <strong>
                            All policies attached to this user, through direct attachment or via group memberships
                            </strong>
                        </small>
                    </p>
                </ActionGroup>

                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>

            <div className="mt-2">
                {content}
            </div>
        </>
    );
};

const UserEffectivePoliciesContainer = () => {
    const router = useRouter();
    const { after } = router.query;
    const { userId } = router.params;
    return (!userId) ? <></> : <UserEffectivePoliciesList
        userId={userId}
        after={(after) ? after : ""}
        onPaginate={after => router.push({pathname: '/auth/users/:userId/policies/effective', params: {userId}, query: {after}})}
    />;
};

const UserEffectivePoliciesPage = () => {
    return (
        <AuthLayout activeTab="users">
            <UserEffectivePoliciesContainer/>
        </AuthLayout>
    );
};

export default UserEffectivePoliciesPage;
