import React from "react";
import { Navigate, Outlet } from "react-router-dom";
import useUser from "../hooks/user";
import {Loading} from "./controls";
import {AUTH_STATUS, useAuth} from "../auth/authContext";

const RequiresAuth: React.FC = () => {
    const { user, loading } = useUser();
    const { status } = useAuth();

    if (loading || status === AUTH_STATUS.UNKNOWN) return <Loading />;

    if (status === AUTH_STATUS.UNAUTHENTICATED && !user) {
        const next = location.pathname + (location.search || "") + (location.hash || "");
        return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    }

    return <Outlet />;
};

export default RequiresAuth;