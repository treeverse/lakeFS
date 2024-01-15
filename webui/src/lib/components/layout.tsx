import React, {FC, useState} from "react";
import { Outlet, useOutletContext } from "react-router-dom";
import { StorageConfigProvider } from "../hooks/storageConfig";

import TopNav from './navbar';

type LayoutOutletContext = [(isLoggedIn: boolean) => void];

const Layout: FC<{logged: boolean}> = ({ logged }) => {
    const [isLogged, setIsLogged] = useState(logged ?? true);
    return (
        <>
            <TopNav logged={isLogged}/>
            <div className="main-app">
                <StorageConfigProvider>
                    <Outlet context={[setIsLogged] satisfies LayoutOutletContext} />
                </StorageConfigProvider>
            </div>
        </>
    );
};

export function useLayoutOutletContext() {
    return useOutletContext<LayoutOutletContext>();
}

export default Layout;
