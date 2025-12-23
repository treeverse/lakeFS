export interface MergeParams {
    message?: string;
    metadata?: { [key: string]: string };
    strategy?: string;
    force?: boolean;
    allow_empty?: boolean;
    squash_merge?: boolean;
}

export interface MergeResult {
    reference: string;
}

export interface PluginMergeOperation {
    merge(
        repoId: string,
        sourceRef: string,
        destinationBranch: string,
        params: MergeParams
    ): Promise<MergeResult>;
}
