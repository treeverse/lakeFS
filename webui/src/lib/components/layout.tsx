import React, { FC, useContext, useEffect } from "react";
import { Outlet } from "react-router-dom";
import { ConfigProvider } from "../hooks/configProvider";
import TopNav from "./navbar";
import { AppContext } from "../hooks/appContext";
import { AUTH_STATUS, useAuth } from "../auth/authContext";

const Layout: FC = () => {
    const { status } = useAuth();
    const showTopNav = status === AUTH_STATUS.AUTHENTICATED;

    const { state } = useContext(AppContext);
    useEffect(() => {
        document.documentElement.setAttribute(
            "data-bs-theme",
            state.settings.darkMode ? "dark" : "light",
        );
    }, [state.settings.darkMode]);

    return (
        <ConfigProvider>
            {showTopNav && <TopNav />}
            <div className="main-app">
                <Outlet />
            </div>
        </ConfigProvider>
    );
};

export default Layout;
