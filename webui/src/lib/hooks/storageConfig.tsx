import React, {
  FC,
  useContext,
  useState,
  createContext,
  useEffect,
} from "react";

import { config } from "../api";

type StorageConfigContextType = {
  error: Error | null;
  loading: boolean;
  blockstore_namespace_ValidityRegex: string | null;
  blockstore_namespace_example: string | null;
  blockstore_type: string | null;
  default_namespace_prefix: string | null;
  import_support: boolean;
  pre_sign_support: boolean;
  pre_sign_support_UI: boolean;
};

const storageConfigInitialState: StorageConfigContextType = {
  error: null,
  loading: true,
  blockstore_namespace_ValidityRegex: null,
  blockstore_namespace_example: null,
  blockstore_type: null,
  default_namespace_prefix: null,
  import_support: false,
  pre_sign_support: false,
  pre_sign_support_UI: false,
};

export const fetchStorageConfig = async () => {
  const storageConfig = await config.getStorageConfig();
  return storageConfig;
};

const StorageConfigContext = createContext<StorageConfigContextType>(
  storageConfigInitialState
);

export const useStorageConfig = () => {
  const config = useContext(StorageConfigContext);
  return config;
};

export const StorageConfigProvider: FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [storageConfig, setStorageConfig] = useState<StorageConfigContextType>(
    storageConfigInitialState
  );

  useEffect(() => {
    const fetchStorageConfigAndSetState = async () => {
      const storageConfig = await fetchStorageConfig();
      setStorageConfig(storageConfig);
    };
    fetchStorageConfigAndSetState();
  }, []);

  return (
    <StorageConfigContext.Provider value={storageConfig}>
      {children}
    </StorageConfigContext.Provider>
  );
};
