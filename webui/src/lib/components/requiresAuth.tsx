import React from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import useUser from "../hooks/user";
import { Loading } from "./controls";

const RequiresAuth: React.FC = () => {
    const { user, loading, checked } = useUser();
    const location = useLocation();

    if (loading || !checked) return <Loading />;

    if (!user) {
        const next = location.pathname + (location.search || "") + (location.hash || "");
        return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    }

    return <Outlet />;
};

export default RequiresAuth;