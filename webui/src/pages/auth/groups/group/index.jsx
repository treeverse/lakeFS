import React from "react";

import {Redirect, Route, Switch} from "react-router-dom";
import {useRouter} from "../../../../lib/hooks/router";
import GroupMembersPage from "./members";
import GroupPoliciesPage from "./policies";


const GroupRedirect = ({ subPath }) => {
    const router = useRouter();
    const { groupId } = router.params;
    return <Redirect to={`/auth/groups/${groupId}${subPath}`}/>;
};

export default function GroupPage() {
    return (
        <Switch>
            <Route exact path="/auth/groups/:groupId">
                <GroupRedirect subPath="/members"/>
            </Route>
            <Route path="/auth/groups/:groupId/members">
                <GroupMembersPage/>
            </Route>
            <Route path="/auth/groups/:groupId/policies">
                <GroupPoliciesPage/>
            </Route>
        </Switch>
    );
}
