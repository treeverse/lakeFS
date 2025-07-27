import React, { createContext, FC, useContext, useEffect, useState, } from "react";

import { config } from "../api";
import useUser from "./user";

type ConfigContextType = {
    error: Error | null;
    loading: boolean;
    config: ConfigType | null;
};

type ConfigType = {
    storages: StorageConfigType[] | null;
    versionConfig?: VersionConfig;
}

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

type VersionConfig = {
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
    const {user} = useUser();
    const [storageConfig, setConfig] = useState<ConfigContextType>(configInitialState);

    useEffect(() => {
        config.getConfig()
            .then(configData =>
                setConfig({config: configData, loading: false, error: null}))
            .catch((error) =>
                setConfig({config: null, loading: false, error}));
    }, [user]);

    return (
        <configContext.Provider value={storageConfig}>
            {children}
        </configContext.Provider>
    );
};

export { ConfigProvider, useConfigContext };