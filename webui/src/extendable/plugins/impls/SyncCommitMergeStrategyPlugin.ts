import { commits, refs } from "../../../lib/api";
import {
    CommitParams,
    CommitResult,
    MergeParams,
    MergeResult,
    PluginCommitMergeStrategy
} from "../pluginCommitMergeStrategy";

class SyncCommitMergeStrategyPlugin implements PluginCommitMergeStrategy {
    async commit(
        repoId: string,
        branchId: string,
        params: CommitParams
    ): Promise<CommitResult> {
        return await commits.commit(
            repoId,
            branchId,
            params.message,
            params.metadata
        );
    }

    async merge(
        repoId: string,
        sourceRef: string,
        destinationBranch: string,
        params: MergeParams
    ): Promise<MergeResult> {
        return await refs.merge(
            repoId,
            sourceRef,
            destinationBranch,
            params.strategy,
            params.message,
            params.metadata
        );
    }
}

export default new SyncCommitMergeStrategyPlugin();
