import React, { useEffect } from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { Loading } from "./controls";
import useUser from "../hooks/user";

const LOGIN_COOKIE_NAMES = ["internal_auth_session", "oidc_auth_session"];
const hasSessionCookie = () => {
    const c = typeof document === "undefined" ? "" : document.cookie || "";
    return LOGIN_COOKIE_NAMES.some((name) => c.includes(`${name}=`));
};

const RequiresAuth: React.FC = () => {
    const { user, loading } = useUser();
    const location = useLocation();
    const next = location.pathname + (location.search || "") + (location.hash || "");

    useEffect(() => {
        const onPageShow = (e: PageTransitionEvent) => {
            const isBFCache =
                (e).persisted ||
                (performance.getEntriesByType("navigation")[0] as PerformanceNavigationTiming | undefined)?.type === "back_forward";

            if (isBFCache && !hasSessionCookie()) {
                window.location.replace("/auth/login?redirected=true");
            }
        };

        window.addEventListener("pageshow", onPageShow);
        return () => window.removeEventListener("pageshow", onPageShow);
    }, []);

    if (loading) return <Loading />;
    if (!user) return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;

    return <Outlet />;
};

export default RequiresAuth;