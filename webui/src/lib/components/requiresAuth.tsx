import React from "react";
import {Navigate, Outlet, useLocation} from "react-router-dom";
import {AUTH_STATUS, useAuth} from "../auth/authContext";

export default function RequiresAuth() {
    const {status} = useAuth();
    const location = useLocation();

    if (status === AUTH_STATUS.UNAUTHENTICATED) {
        // Save the location the user was trying to go
        const next = location.pathname + (location.search || "");
        return <Navigate to="/auth/login" replace state={{ next, redirected: true }} />;
    }

    // Here the user is authenticated
    return <Outlet/>;
}
