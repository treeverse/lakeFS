import React, {FC, useState} from "react";
import { Outlet, useOutletContext } from "react-router-dom";
import { StorageConfigProvider } from "../hooks/storageConfig";

import TopNav from './navbar';

type LayoutOutletContext = [(isLoggedIn: boolean) => void];

function applyDarkThemeWhenChanged() {
    const keyIsDarkMode = "is_dark_mode";

    function setDarkThemeFromLocalStorage() {
        const isDarkMode = window.localStorage.getItem(keyIsDarkMode) === String(true)
        document.documentElement.setAttribute('data-bs-theme', isDarkMode ? 'dark' : 'light')
    }

    setDarkThemeFromLocalStorage(); // initial set of Dark Mode

    window.addEventListener("storage", (event) => {
        if (event.key === keyIsDarkMode) {
            setDarkThemeFromLocalStorage();
        }
    });
}

const Layout: FC<{logged: boolean}> = ({ logged }) => {
    const [isLogged, setIsLogged] = useState(logged ?? true);

    applyDarkThemeWhenChanged();

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
