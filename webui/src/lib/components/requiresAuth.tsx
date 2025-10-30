import React from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { Loading } from "./controls";
import { useAuth, AUTH_STATUS } from "../auth/authContext";

const RequiresAuth: React.FC = () => {
    const { status } = useAuth();
    const location = useLocation();
    const next = location.pathname + (location.search || "") + (location.hash || "");

    if (status === AUTH_STATUS.UNKNOWN) return <Loading />;
    if (status === AUTH_STATUS.UNAUTHENTICATED) return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;

    return <Outlet />;
};

export default RequiresAuth;