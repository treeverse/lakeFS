export interface CommitParams {
    message: string;
    metadata?: { [key: string]: string };
    date?: number;
    allow_empty?: boolean;
    force?: boolean;
    source_metarange?: string;
}

export interface MergeParams {
    message?: string;
    metadata?: { [key: string]: string };
    strategy?: string;
    force?: boolean;
    allow_empty?: boolean;
    squash_merge?: boolean;
}

export interface CommitResult {
    id: string;
    parents: string[];
    committer: string;
    message: string;
    creation_date: number;
    meta_range_id: string;
    metadata?: { [key: string]: string };
    generation?: number;
    version?: number;
}

export interface MergeResult {
    reference: string;
}

export interface PluginCommitMergeStrategy {
    commit(
        repoId: string,
        branchId: string,
        params: CommitParams
    ): Promise<CommitResult>;

    merge(
        repoId: string,
        sourceRef: string,
        destinationBranch: string,
        params: MergeParams
    ): Promise<MergeResult>;
}
