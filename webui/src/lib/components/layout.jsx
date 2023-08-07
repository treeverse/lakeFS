import React, {useState} from "react";
import { Outlet } from "react-router-dom";

import TopNav from './navbar';

const Layout = ({ logged }) => {
    const [isLogged, setIsLogged] = useState(logged ?? true);
    return (
        <>
            <TopNav logged={isLogged}/>
            <div className="main-app">
                <Outlet context={[setIsLogged]} />
            </div>
        </>
    );
};

export default Layout;