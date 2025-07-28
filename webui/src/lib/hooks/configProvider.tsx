import React, { createContext, FC, useContext, useEffect, useState, } from "react";

import { config } from "../api";
import useUser from "./user";
import { usePluginManager } from "../../extendable/plugins/pluginsContext";

type ConfigContextType = {
    error: Error | null;
    loading: boolean;
    config: ConfigType | null;
};

type ConfigType = {
    storages: StorageConfigType[] | null;
    customization: CustomizationType | null;
    versionConfig?: VersionConfig;
};

type StorageConfigType = {
    blockstore_namespace_ValidityRegex: string | null;
    blockstore_namespace_example: string | null;
    blockstore_type: string | null;
    default_namespace_prefix: string | null;
    import_support: boolean;
    import_validity_regex: string | null;
    pre_sign_support: boolean;
    pre_sign_support_ui: boolean;
};

type CustomizationType = {
    ui: UiCustomizationType | null;
};

type UiCustomizationType = {
    custom_viewers: Array<CustomViewerConfig> | null;
};

type CustomViewerConfig = {
    name: string;
    url: string;
    content_types: Array<string> | null;
    extensions: Array<string> | null;
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
    const {user} = useUser();
    const [storageConfig, setConfig] = useState<ConfigContextType>(configInitialState);

    useEffect(() => {
        config.getConfig()
            .then(configData => {
                pluginManager.customObjectRenderers?.init(configData);
                setConfig({config: configData, loading: false, error: null});
            })
            .catch((error) =>
                setConfig({config: null, loading: false, error}));
    }, [user]);

    return (
        <configContext.Provider value={storageConfig}>
            {children}
        </configContext.Provider>
    );
};

export type { ConfigType, CustomViewerConfig };

export { ConfigProvider, useConfigContext };
