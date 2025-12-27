import React from "react";
import { Navigate, Outlet } from "react-router-dom";
import { Loading } from "./controls";
import { ROUTES } from "../utils";
import { getCurrentRelativeUrl } from "../utils";
import { AUTH_STATUS, useAuth } from "../auth/authContext";

const RequiresAuth: React.FC = () => {
    const { user, status } = useAuth();

    if (status === AUTH_STATUS.PENDING) return <Loading />;
    if (!user) {
        const next = getCurrentRelativeUrl();
        const params = new URLSearchParams({ redirected: "true", next });
        return (
            <Navigate
                to={{ pathname: ROUTES.LOGIN, search: `?${params.toString()}` }}
                replace
                state={{ redirected: true, next }}
            />
        );
    }

    return <Outlet />;
};

export default RequiresAuth;
