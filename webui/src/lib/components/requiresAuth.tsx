import React, { useEffect } from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { Loading } from "./controls";
import useUser from "../hooks/user";

const RequiresAuth: React.FC = () => {
    const { user, loading } = useUser();
    const location = useLocation();
    const next = location.pathname + (location.search || "") + (location.hash || "");

    useEffect(() => {
        const noop = () => {};
        window.addEventListener("unload", noop);
        return () => window.removeEventListener("unload", noop);
    }, []);

    if (loading) return <Loading />;
    if (!user) return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;

    return <Outlet />;
};

export default RequiresAuth;