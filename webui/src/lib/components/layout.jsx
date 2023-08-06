import React from "react";
import { Outlet } from "react-router-dom";

import TopNav from './navbar';

const Layout = ({ logged = true }) => {
    return (
        <>
            <TopNav logged={logged}/>
            <div className="main-app">
                <Outlet />
            </div>
        </>
    );
};

export default Layout;
