import React, { createContext, FC, useContext, useEffect, useState, } from "react";

import { config } from "../api";
import useUser from "./user";

type StorageConfigContextType = {
    error: Error | null;
    loading: boolean;
    configs: [StorageConfigType] | null;
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

const storageConfigInitialState: StorageConfigContextType = {
    error: null,
    loading: true,
    configs: null,
};

const StorageConfigContext = createContext<StorageConfigContextType>(storageConfigInitialState);

const useStorageConfigs = () => useContext(StorageConfigContext);

const StorageConfigProvider: FC<{children: React.ReactNode}> = ({children}) => {
    const {user} = useUser();
    const [storageConfig, setStorageConfig] = useState<StorageConfigContextType>(storageConfigInitialState);

    useEffect(() => {
        config.getStorageConfigs()
            .then(configs =>
                setStorageConfig({configs, loading: false, error: null}))
            .catch((error) =>
                setStorageConfig({configs: null, loading: false, error}));
    }, [user]);

    return (
        <StorageConfigContext.Provider value={storageConfig}>
            {children}
        </StorageConfigContext.Provider>
    );
};

export { StorageConfigProvider, useStorageConfigs };