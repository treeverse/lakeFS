import React from "react";
import {Navigate, Outlet, useLocation} from "react-router-dom";
import {Loading} from "./controls";
import useUser from "../hooks/user";

const RequiresAuth: React.FC = () => {
    const {user, loading} = useUser();
    const location = useLocation();

    if (loading) return <Loading/>;
    if (!user) {
        const next = location.pathname + (location.search || "") + (location.hash || "");
        const url = `/auth/login?redirected=true&next=${encodeURIComponent(next)}`;
        return <Navigate to={url} replace state={{ next, redirected: true }} />;
    }

    return <Outlet/>;
};

export default RequiresAuth;