import React from "react";

import {Navigate, Route, Routes} from "react-router-dom";
import CredentialsPage from "./credentials";
import GroupsIndexPage from "./groups";
import UsersIndexPage from "./users";
import PoliciesIndexPage from "./policies";
import LoginPage from "./login";
import ResetPasswordPage from "./reset-password";
import ActivateInvitedUserPage from "./users/create-user-with-password";
import {useSimplified} from '../context.jsx';

const Auth = () => {
    const simplifiedPolicies = useSimplified();
    return (
        <Routes>
            <Route path="" element={<Navigate to="credentials"/>} />
            <Route path="login" element={<LoginPage/>} />
            <Route path="resetpassword" element={<ResetPasswordPage/>} />
            <Route path="credentials" element={<CredentialsPage/>} />
            <Route path="users/create" element={<ActivateInvitedUserPage/>} />
            <Route path="users/*" element={<UsersIndexPage/>} />
            <Route path="groups/*" element={<GroupsIndexPage/>} />
            <Route path="policies/*" element={<PoliciesIndexPage/>} hidden={simplifiedPolicies} />
        </Routes>
    )
}

export default Auth;
