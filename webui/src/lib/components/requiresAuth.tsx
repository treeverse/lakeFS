import React from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import useUser from "../hooks/user";
import { Loading } from "./controls";
import { AUTH_STATUS, useAuth } from "../auth/authContext";

const RequiresAuth: React.FC = () => {
    const { status } = useAuth();
    const { user, loading } = useUser(); // נשאר כפי שהוא
    const location = useLocation();

    if (status === AUTH_STATUS.UNAUTHENTICATED) {
        const next = location.pathname + (location.search || "") + (location.hash || "");
        return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    }

    if (status === AUTH_STATUS.UNKNOWN || loading) {
        return <Loading />;
    }

    if (!user) {
        const next = location.pathname + (location.search || "") + (location.hash || "");
        return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    }

    return <Outlet />;
};

export default RequiresAuth;