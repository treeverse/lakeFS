import React from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import useUser from "../hooks/user";
import { Loading } from "./controls";

function hasAuthSessionCookie() {
    if (typeof document === "undefined") return false;
    const c = document.cookie;
    return c.includes("internal_auth_session=") || c.includes("oidc_auth_session=");
}

const RequiresAuth: React.FC = () => {
    const { user, loading } = useUser();
    const location = useLocation();

    if (!hasAuthSessionCookie()) {
        const next = location.pathname + (location.search || "") + (location.hash || "");
        return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    }

    if (loading) return <Loading />;
    if (!user) {
        const next = location.pathname + (location.search || "") + (location.hash || "");
        return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    }

    return <Outlet />;
};

export default RequiresAuth;