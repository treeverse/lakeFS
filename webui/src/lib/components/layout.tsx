import React, {FC, useContext, useState} from "react";
import { Outlet, useOutletContext } from "react-router-dom";
import { StorageConfigProvider } from "../hooks/storageConfig";
import TopNav from './navbar';
import { AppContext } from "../hooks/appContext";

type LayoutOutletContext = [(isLoggedIn: boolean) => void];

const Layout: FC<{logged: boolean}> = ({ logged }) => {
    const [isLogged, setIsLogged] = useState(logged ?? true);

    // handle global dark mode here
    const {state} = useContext(AppContext);
    document.documentElement.setAttribute('data-bs-theme', state.settings.darkMode ? 'dark' : 'light')

    return (
        <>
            {/* Sticky license/banner alert placeholder (conditionally rendered in Enterprise) */}
            <div id="license-banner-placeholder" className="license-banner-sticky" style={{display: 'none'}}></div>
            <TopNav logged={isLogged}/>
            <div className="main-app">
                <StorageConfigProvider>
                    <Outlet context={[setIsLogged] satisfies LayoutOutletContext}/>
                </StorageConfigProvider>
            </div>
        </>
    );
};

export function useLayoutOutletContext() {
    return useOutletContext<LayoutOutletContext>();
}

export default Layout;
