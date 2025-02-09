import React, { createContext, useContext } from 'react';
import { PluginManager } from "./pluginManager";

const PluginsContext = createContext<PluginManager | undefined>(undefined);

interface PluginManagerProviderProps {
    pluginManager: PluginManager;
    children: React.ReactNode;
}

export const PluginManagerProvider: React.FC<PluginManagerProviderProps> = ({pluginManager, children}) => {
    return (
        <PluginsContext.Provider value={pluginManager}>
            {children}
        </PluginsContext.Provider>
    );
};

export const usePluginManager = (): PluginManager => {
    const context = useContext(PluginsContext);
    if (!context) {
        throw new Error('usePluginManager must be used within a PluginManagerProvider');
    }
    return context;
};
