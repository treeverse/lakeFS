import React, { useEffect } from "react";
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
    const noSession = !hasAuthSessionCookie();

    if (noSession) return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    if (loading) return <Loading />;
    if (!user) return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;

    useEffect(() => {
        const onPageShow = (e: PageTransitionEvent) => {
            if ((e).persisted && !hasAuthSessionCookie()) {
                window.location.replace(`/auth/login?redirected=true&next=${encodeURIComponent(next)}`);
            }
        };
        window.addEventListener("pageshow", onPageShow);
        return () => window.removeEventListener("pageshow", onPageShow);
    }, []);

    return <Outlet />;
};

export default RequiresAuth;