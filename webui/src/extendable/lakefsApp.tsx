import React from 'react';
import { PluginManager } from "./plugins/pluginManager";
import { IndexPage } from "../pages";
import { PluginManagerProvider } from "./plugins/pluginsContext";

interface AppProps {
    pluginManager: PluginManager;
}

const LakeFSApp: React.FC<AppProps> = ({pluginManager}) => {
    return (
        <PluginManagerProvider pluginManager={pluginManager}>
            <IndexPage/>
        </PluginManagerProvider>
    );
};

export default LakeFSApp;