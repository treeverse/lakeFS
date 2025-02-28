function getRepoStorageConfig(configs, repo) {
    if (!configs || !configs.length) {
        return {storageConfig: null, error: new Error('No storage configs found')};
    }

    const storageID = repo?.storage_id;
    if (storageID) {
        // find the storage config that matches the repo
        const storageConfig = configs.find(c => c['blockstore_id'] === storageID);
        if (!storageConfig) {
            return {storageConfig: null, error: new Error('No storage config found for repo. StorageID: ' + storageID)};
        }
        return {storageConfig, error: null};
    } else {
        if (configs.length > 1) {
            return {storageConfig: null, error: new Error('Repo with no StorageID, cannot match storage config')};
        }
        // single blockstore config
        return {storageConfig: configs[0], error: null};
    }
}

export {getRepoStorageConfig};
