export interface MergeResult {
    reference: string;
}

export interface PluginMergeOperation {
    merge(
        repoId: string,
        sourceRef: string,
        destinationBranch: string,
        strategy?: string,
        message?: string,
        metadata?: { [key: string]: string }
    ): Promise<MergeResult>;
}
