import React from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { Loading } from "./controls";
import useUser from "../hooks/user";

const LOGIN_COOKIE_NAMES = ["internal_auth_session", "oidc_auth_session"];
const hasSessionCookie = () => {
    const c = typeof document === "undefined" ? "" : document.cookie || "";
    return LOGIN_COOKIE_NAMES.some((name) => c.includes(`${name}=`));
};

declare global { interface Window { __lakefsBFGuard?: boolean } }
if (typeof window !== "undefined" && !window.__lakefsBFGuard) {
    const redirectIfUnauthed = () => {
        const path = location.pathname;
        const isAuthArea = path.startsWith("/auth/");
        if (!isAuthArea && !hasSessionCookie()) {
            window.location.replace("/auth/login?redirected=true");
        }
    };
    window.addEventListener("popstate", redirectIfUnauthed, { capture: true });
    const onPageShow = (e: any) => {
        const type = (performance.getEntriesByType("navigation")[0] as PerformanceNavigationTiming | undefined)?.type;
        if (e?.persisted === true || type === "back_forward") redirectIfUnauthed();
    };
    window.addEventListener("pageshow", onPageShow, { capture: true });
    window.__lakefsBFGuard = true;
}

const RequiresAuth: React.FC = () => {
    const {user, loading} = useUser();
    const location = useLocation();
    const next = location.pathname + (location.search || "") + (location.hash || "");

    if (loading) return <Loading/>;
    if (!user) return <Navigate to="/auth/login" replace state={{next, redirected: true}}/>;

    return <Outlet/>;
};

export default RequiresAuth;