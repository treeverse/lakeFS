import React, {createContext, FC, useContext, useEffect, useMemo} from "react";

import { config } from "../api";
import { usePluginManager } from "../../extendable/plugins/pluginsContext";
import {useAPI} from "./api";
import {useAuth} from "../auth/authContext";

type ConfigContextType = {
    error: Error | null;
    loading: boolean;
    config: ConfigType | null;
};

type ConfigType = {
    storages?: StorageConfig[];
    uiConfig?: UIConfig;
    versionConfig?: VersionConfig;
};

type StorageConfig = {
    blockstore_namespace_ValidityRegex: string | null;
    blockstore_namespace_example: string | null;
    blockstore_type: string | null;
    default_namespace_prefix: string | null;
    import_support: boolean;
    import_validity_regex: string | null;
    pre_sign_support: boolean;
    pre_sign_support_ui: boolean;
};

type UIConfig = {
    custom_viewers?: Array<CustomViewer>;
};

type CustomViewer = {
    name: string;
    url: string;
    content_types?: Array<string>;
    extensions?: Array<string>;
};

type VersionConfig = {
    upgrade_recommended?: boolean;
    upgrade_url?: string;
    version_context?: string;
    version?: string;
};

const configInitialState: ConfigContextType = {
    error: null,
    loading: true,
    config: null,
};

const configContext = createContext<ConfigContextType>(configInitialState);

const useConfigContext = () => useContext(configContext);

const ConfigProvider: FC<{children: React.ReactNode}> = ({children}) => {
    const pluginManager = usePluginManager();
    const {user} = useAuth();
    const { response, loading, error } = useAPI(() => config.getConfig(), [user]);

    useEffect(() => {
        if (response) {
            pluginManager.customObjectRenderers?.init(response);
        }
    }, [response, pluginManager]);

    const value = useMemo(
        () => (
            { config: response ?? null, loading, error } satisfies ConfigContextType
        ),
        [response, loading, error]);

    return (
        <configContext.Provider value={value}>
            {children}
        </configContext.Provider>
    );
};

export type { ConfigType, CustomViewer };

export { ConfigProvider, useConfigContext };
