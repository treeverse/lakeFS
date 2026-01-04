import { commits } from '../../../lib/api';
import { CommitResult, PluginCommitOperation } from '../pluginCommitOperation';
import { CapabilitiesConfig } from '../../../lib/hooks/configProvider';

class SyncCommitPlugin implements PluginCommitOperation {
    async commit(
        repoId: string,
        branchId: string,
        message: string,
        metadata?: { [key: string]: string },
        _capabilitiesConfig?: CapabilitiesConfig,
    ): Promise<CommitResult> {
        return commits.commit(repoId, branchId, message, metadata);
    }
}

export default new SyncCommitPlugin();
