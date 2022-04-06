import React from "react";

import {Redirect, Route, Switch} from "react-router-dom";
import CredentialsPage from "./credentials";
import GroupsIndexPage from "./groups";
import UsersIndexPage from "./users";
import PoliciesIndexPage from "./policies";
import LoginPage from "./login";
import ResetPasswordPage from "./reset-password";

const Auth = () => {
    return (
        <Switch>
            <Route exact path="/auth">
                <Redirect to="/auth/credentials"/>
            </Route>
            <Route path="/auth/login">
                <LoginPage/>
            </Route>
            <Route path="/auth/resetpassword">
                <ResetPasswordPage/>
            </Route>

            <Route path="/auth/credentials">
                <CredentialsPage/>
            </Route>
            <Route path="/auth/users">
                <UsersIndexPage/>
            </Route>
            <Route path="/auth/groups">
                <GroupsIndexPage/>
            </Route>
            <Route path="/auth/policies">
                <PoliciesIndexPage/>
            </Route>
        </Switch>
    )
}

export default Auth;
