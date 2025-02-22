function getRepoStorageConfig(configs, repo) {
    if (!configs || !configs.length) {
        return {storageConfig: null, error: new Error('No storage configs found')};
    }

    const storageID = repo?.storage_id;
    if (storageID) {
        // find the storage config that matches the repo
        const storageConfig = configs.find(c => c['blockstore_id'] === storageID);
        if (storageConfig) {
            return {storageConfig, error: null};
        } else {
            return {storageConfig: null, error: new Error('No storage config found for repo')};
        }
    } else {
        if (configs.length === 1) {
            // single blockstore config
            return {storageConfig: configs[0], error: null};
        } else {
            // fallback to backward compatible config
            const storageConfig = configs.find(c => c['backward_compatible']);
            if (storageConfig) {
                return {storageConfig, error: null};
            } else {
                return {storageConfig: null, error: new Error('No backward compatible storage config found')};
            }
        }
    }
}

export {getRepoStorageConfig};
