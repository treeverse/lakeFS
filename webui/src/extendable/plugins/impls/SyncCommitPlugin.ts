import { commits } from "../../../lib/api";
import {
    CommitParams,
    CommitResult,
    PluginCommitOperation
} from "../pluginCommitOperation";

class SyncCommitPlugin implements PluginCommitOperation {
    async commit(
        repoId: string,
        branchId: string,
        params: CommitParams
    ): Promise<CommitResult> {
        const message = params.message || undefined;
        const metadata = params.metadata && Object.keys(params.metadata).length > 0
            ? params.metadata
            : undefined;

        return await commits.commit(
            repoId,
            branchId,
            message,
            metadata
        );
    }
}

export default new SyncCommitPlugin();
