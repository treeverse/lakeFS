import React from "react";

import {Navigate, Route, Routes} from "react-router-dom";
import CredentialsPage from "./credentials";
import GroupsIndexPage from "./groups";
import UsersIndexPage from "./users";
import PoliciesIndexPage from "./policies";
import LoginPage from "./login";
import ActivateInvitedUserPage from "./users/create-user-with-password";

const Auth = () => {
    return (
        <Routes>
            <Route path="" element={<Navigate to="credentials"/>} />
            <Route path="login" element={<LoginPage/>} />
            <Route path="credentials" element={<CredentialsPage/>} />
            <Route path="users/create" element={<ActivateInvitedUserPage/>} />
            <Route path="users/*" element={<UsersIndexPage/>} />
            <Route path="groups/*" element={<GroupsIndexPage/>} />
            <Route path="policies/*" element={<PoliciesIndexPage/>} />
        </Routes>
    )
}

export default Auth;
