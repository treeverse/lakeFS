import React from "react";
import {Navigate, Outlet, useLocation} from "react-router-dom";
import {AUTH_STATUS, useAuth} from "../auth/authContext";
import {auth} from "../api";
import {useAPI} from "../hooks/api";

const RequiresAuth = () => {
    const location = useLocation();
    const { status, setAuthStatus } = useAuth();

    const { response: user, loading } = useAPI(() => auth.getCurrentUser(), [location.key]);

    if (loading) return null;

    if (user) {
        if (status === AUTH_STATUS.UNAUTHENTICATED) {
            setAuthStatus(AUTH_STATUS.AUTHENTICATED);
        }
        return <Outlet />;
    }

    const next = location.pathname + (location.search || "");
    return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
};

export default RequiresAuth;
