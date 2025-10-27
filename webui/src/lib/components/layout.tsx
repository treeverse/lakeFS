import React, { FC, useContext, useState, useEffect } from "react";
import { Outlet, useOutletContext } from "react-router-dom";
import { ConfigProvider } from "../hooks/configProvider";
import TopNav from './navbar';
import { AppContext } from "../hooks/appContext";
import useUser from "../hooks/user";

type LayoutOutletContext = [boolean];

const Layout: FC<{logged: boolean}> = ({logged}) => {
    const [isLogged, setIsLogged] = useState(logged ?? true);
    const { user, loading, error } = useUser();
    const userWithId = user as { id?: string } | null;

    // Update isLogged state based on actual authentication status
    useEffect(() => {
        if (!loading) {
            // If there's a user and no error, show authenticated (full) navbar
            setIsLogged(!!userWithId?.id && !error);
        }
    }, [userWithId, loading, error]);

    // handle global dark mode here
    const {state} = useContext(AppContext);
    document.documentElement.setAttribute('data-bs-theme', state.settings.darkMode ? 'dark' : 'light')

    return (
        <ConfigProvider>
            {!loading && <TopNav logged={isLogged}/>}
            <div className="main-app">
                <Outlet context={[isLogged] satisfies LayoutOutletContext}/>
            </div>
        </ConfigProvider>
    );
};

export function useLayoutOutletContext() {
    return useOutletContext<LayoutOutletContext>();
}

export default Layout;
