import { refs } from '../../../lib/api';
import { MergeResult, PluginMergeOperation } from '../pluginMergeOperation';
import { CapabilitiesConfig } from '../../../lib/hooks/configProvider';

class SyncMergePlugin implements PluginMergeOperation {
    async merge(
        repoId: string,
        sourceRef: string,
        destinationBranch: string,
        strategy?: string,
        message?: string,
        metadata?: { [key: string]: string },
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        capabilitiesConfig?: CapabilitiesConfig,
    ): Promise<MergeResult> {
        return await refs.merge(repoId, sourceRef, destinationBranch, strategy, message, metadata);
    }
}

export default new SyncMergePlugin();
