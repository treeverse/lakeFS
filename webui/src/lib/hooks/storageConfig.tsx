import React, {createContext, FC, useContext, useEffect, useState} from "react";

import {config} from "../api";
import useUser from "./user";

type StorageConfigContextType = {
    error: Error | null;
    loading: boolean;
    configs: [StorageConfigType] | null;
    refresh: () => void;
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
    refresh: () => {
    },
};

const StorageConfigContext = createContext<StorageConfigContextType>(storageConfigInitialState);

const useStorageConfigs = () => useContext(StorageConfigContext);

const StorageConfigProvider: FC<{ children: React.ReactNode }> = ({children}) => {
    const {user} = useUser();
    const [refreshTrigger, setRefreshTrigger] = useState(0);
    const [storageConfig, setStorageConfig] = useState<StorageConfigContextType>({
        ...storageConfigInitialState,
        refresh: () => setRefreshTrigger((prev) => prev + 1),
    });

    useEffect(() => {
        config.getStorageConfigs()
            .then((configs) =>
                setStorageConfig((prev) => ({...prev, configs, loading: false, error: null})),
            )
            .catch((error) =>
                setStorageConfig((prev) => ({...prev, configs: null, loading: false, error})),
            );
    }, [user, refreshTrigger]);

    return (
        <StorageConfigContext.Provider value={storageConfig}>
            {children}
        </StorageConfigContext.Provider>
    );
};

export {StorageConfigProvider, useStorageConfigs};
