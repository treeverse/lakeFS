import React from "react";

import {Redirect, Route, Switch} from "react-router-dom";

import {useRouter} from "../../../../lib/hooks/router";
import UserGroupsPage from "./groups";
import UserCredentialsPage from "./credentials";
import UserEffectivePoliciesPage from "./effectivePolicies";
import UserPoliciesPage from "./policies";


const UserRedirect = ({ subPath }) => {
    const router = useRouter();
    const {userId} = router.params;
    return <Redirect to={`/auth/users/${userId}${subPath}`}/>;
}

export default function UserPage() {
    return (
        <Switch>
            <Route exact path="/auth/users/:userId">
                <UserRedirect subPath="/groups"/>
            </Route>
            <Route exact path="/auth/users/:userId/groups">
                <UserGroupsPage />
            </Route>
            <Route exact path="/auth/users/:userId/policies">
                <UserPoliciesPage />
            </Route>
            <Route exact path="/auth/users/:userId/policies/effective">
                <UserEffectivePoliciesPage />
            </Route>
            <Route exact path="/auth/users/:userId/credentials">
                <UserCredentialsPage />
            </Route>
        </Switch>
    );
}
