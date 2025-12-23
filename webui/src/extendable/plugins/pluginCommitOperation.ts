export interface CommitParams {
    message: string;
    metadata?: { [key: string]: string };
    date?: number;
    allow_empty?: boolean;
    force?: boolean;
    source_metarange?: string;
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

export interface PluginCommitOperation {
    commit(
        repoId: string,
        branchId: string,
        params: CommitParams
    ): Promise<CommitResult>;
}
