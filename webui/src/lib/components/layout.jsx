import React, {useState} from "react";
import { Outlet } from "react-router-dom";
import { StorageConfigProvider } from "../hooks/storageConfig";

import TopNav from './navbar';

const Layout = ({ logged }) => {
    const [isLogged, setIsLogged] = useState(logged ?? true);
    return (
        <>
            <TopNav logged={isLogged}/>
            <div className="main-app">
                <StorageConfigProvider>
                    <Outlet context={[setIsLogged]} />
                </StorageConfigProvider>
            </div>
        </>
    );
};

export default Layout;
