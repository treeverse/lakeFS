import React from "react";

import {Navigate, Route, Routes} from "react-router-dom";

import {useRouter} from "../../../../lib/hooks/router";
import UserGroupsPage from "./groups";
import UserCredentialsPage from "./credentials";
import UserEffectivePoliciesPage from "./effectivePolicies";
import UserPoliciesPage from "./policies";


const UserRedirect = ({ subPath }) => {
    const router = useRouter();
    const {userId} = router.params;
    return <Navigate to={`/auth/users/${userId}${subPath}`}/>;
}

export default function UserPage() {
    return (
        <Routes>
            <Route path="" element={<UserRedirect subPath="/groups"/>} />
            <Route path="groups" element={<UserGroupsPage />} />
            <Route exact path="policies" element={<UserPoliciesPage />} />
            <Route exact path="policies/effective" element={<UserEffectivePoliciesPage />} />
            <Route exact path="credentials" element={<UserCredentialsPage />} />
        </Routes>
    );
}
