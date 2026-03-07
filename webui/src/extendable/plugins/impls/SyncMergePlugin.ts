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
        _capabilitiesConfig?: CapabilitiesConfig,
        squashMerge?: boolean,
    ): Promise<MergeResult> {
        return refs.merge(repoId, sourceRef, destinationBranch, strategy, message, metadata, squashMerge);
    }
}

export default new SyncMergePlugin();
