import React from "react";
import { Navigate, Outlet, useLocation } from "react-router-dom";
import { AUTH_STATUS, useAuth } from "../auth/authContext";
import useUser from "../hooks/user";
import {Loading} from "./controls";

const RequiresAuth: React.FC = () => {
    const { status } = useAuth();
    const { loading } = useUser();
    const location = useLocation();

    if (status === AUTH_STATUS.UNAUTHENTICATED) {
        const next = location.pathname + (location.search || "") + (location.hash || "");
        return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    }

    if (status !== AUTH_STATUS.AUTHENTICATED || loading) {
        return <Loading />;
    }

    return <Outlet />;
};

export default RequiresAuth;
