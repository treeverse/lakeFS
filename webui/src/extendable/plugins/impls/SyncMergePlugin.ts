import { refs } from "../../../lib/api";
import {
    MergeResult,
    PluginMergeOperation
} from "../pluginMergeOperation";

class SyncMergePlugin implements PluginMergeOperation {
    async merge(
        repoId: string,
        sourceRef: string,
        destinationBranch: string,
        strategy?: string,
        message?: string,
        metadata?: { [key: string]: string }
    ): Promise<MergeResult> {
        return await refs.merge(
            repoId,
            sourceRef,
            destinationBranch,
            strategy,
            message,
            metadata
        );
    }
}

export default new SyncMergePlugin();
