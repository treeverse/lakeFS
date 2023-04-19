import React from "react";

import {Navigate, Route, Routes} from "react-router-dom";
import {useRouter} from "../../../../lib/hooks/router";
import GroupMembersPage from "./members";
import GroupPoliciesPage from "./policies";


const GroupRedirect = ({ subPath }) => {
    const router = useRouter();
    const { groupId } = router.params;
    return <Navigate to={`/auth/groups/${groupId}${subPath}`}/>;
};

export default function GroupPage() {
    return (
        <Routes>
            <Route path="" element={<GroupRedirect subPath="/members"/>} />
            <Route path="members" element={<GroupMembersPage/>} />
            <Route path="policies" element={<GroupPoliciesPage/>} />
        </Routes>
    );
}
