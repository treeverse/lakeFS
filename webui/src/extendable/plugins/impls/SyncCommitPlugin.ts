import { commits } from "../../../lib/api";
import {
    CommitResult,
    PluginCommitOperation
} from "../pluginCommitOperation";

class SyncCommitPlugin implements PluginCommitOperation {
    async commit(
        repoId: string,
        branchId: string,
        message: string,
        metadata?: { [key: string]: string }
    ): Promise<CommitResult> {
        return await commits.commit(
            repoId,
            branchId,
            message,
            metadata
        );
    }
}

export default new SyncCommitPlugin();
