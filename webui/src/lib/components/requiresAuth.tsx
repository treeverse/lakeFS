import React from "react";
import {Navigate, Outlet} from "react-router-dom";
import {Loading} from "./controls";
import useUser from "../hooks/user";
import {ROUTES} from "../utils";
import {getCurrentRelativeUrl} from "../utils";

const RequiresAuth: React.FC = () => {
    const {user, loading} = useUser();

    if (loading) return <Loading/>;
    if (!user) {
        const next = getCurrentRelativeUrl();
        const params = new URLSearchParams({ redirected: "true", next });
        return <Navigate to={{ pathname: ROUTES.LOGIN, search: `?${params.toString()}` }} replace state={{ redirected: true, next }}/>;
    }

    return <Outlet/>;
};

export default RequiresAuth;