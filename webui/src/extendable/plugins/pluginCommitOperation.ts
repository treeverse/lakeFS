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
        message: string,
        metadata?: { [key: string]: string },
    ): Promise<CommitResult>;
}
