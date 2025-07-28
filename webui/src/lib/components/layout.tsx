import React, { FC, useContext, useState } from "react";
import { Outlet, useOutletContext } from "react-router-dom";
import { ConfigProvider } from "../hooks/configProvider";
import TopNav from './navbar';
import { AppContext } from "../hooks/appContext";

type LayoutOutletContext = [(isLoggedIn: boolean) => void];

const Layout: FC<{logged: boolean}> = ({logged}) => {
    const [isLogged, setIsLogged] = useState(logged ?? true);

    // handle global dark mode here
    const {state} = useContext(AppContext);
    document.documentElement.setAttribute('data-bs-theme', state.settings.darkMode ? 'dark' : 'light')

    return (
        <ConfigProvider>
            <TopNav logged={isLogged}/>
            <div className="main-app">
                <Outlet context={[setIsLogged] satisfies LayoutOutletContext}/>
            </div>
        </ConfigProvider>
    );
};

export function useLayoutOutletContext() {
    return useOutletContext<LayoutOutletContext>();
}

export default Layout;
