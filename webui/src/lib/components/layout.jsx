import React from "react";

import TopNav from './navbar';

const Layout = ({ logged = true, children }) => {
    return (
        <>
            <TopNav logged={logged}/>
            <div className="main-app">
                {children}
            </div>
        </>
    );
};

export default Layout;