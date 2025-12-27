import { refs } from "../../../lib/api";
import {
    MergeParams,
    MergeResult,
    PluginMergeOperation
} from "../pluginMergeOperation";

class SyncMergePlugin implements PluginMergeOperation {
    async merge(
        repoId: string,
        sourceRef: string,
        destinationBranch: string,
        params: MergeParams
    ): Promise<MergeResult> {
        const message = params.message || undefined;
        const metadata = params.metadata && Object.keys(params.metadata).length > 0
            ? params.metadata
            : undefined;
        const strategy = params.strategy || undefined;

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
