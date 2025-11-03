import React from "react";
import {Navigate, Outlet} from "react-router-dom";
import {Loading} from "./controls";
import useUser from "../hooks/user";
import {buildNextFromWindow, ROUTES} from "../utils";

const RequiresAuth: React.FC = () => {
    const {user, loading} = useUser();

    if (loading) return <Loading/>;
    if (!user) {
        const next = buildNextFromWindow();
        const url = `${ROUTES.LOGIN}?redirected=true&next=${encodeURIComponent(next)}`;
        return <Navigate to={url} replace state={{ next, redirected: true }} />;
    }

    return <Outlet/>;
};

export default RequiresAuth;