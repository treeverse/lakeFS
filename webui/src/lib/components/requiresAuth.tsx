import React from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { Loading } from "./controls";
import useUser from "../hooks/user";

const hasAuthSessionCookie = () => {
    const c = typeof document === "undefined" ? "" : document.cookie || "";
    return c.includes("internal_auth_session=") || c.includes("oidc_auth_session=");
}

const RequiresAuth: React.FC = () => {
    const { user, loading } = useUser();
    const location = useLocation();
    const next = location.pathname + (location.search || "") + (location.hash || "");

    if (!hasAuthSessionCookie()) return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    if (loading) return <Loading />;
    if (!user) return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;

    return <Outlet />;
};

export default RequiresAuth;