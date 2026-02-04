import { PluginRepoCreationForm } from './pluginRepoCreationForm';
import DefaultRepoCreationFormPlugin from './impls/DefaultRepoCreationFormPlugin';
import { PluginCustomObjectRenderers } from './pluginCustomObjectRenderers';
import DefaultCustomObjectRenderersPlugin from './impls/DefaultCustomObjectRenderers';
import { PluginLoginStrategy } from './pluginLoginStrategy';
import RedirectToSSOStrategyPlugin from './impls/RedirectToSSOStrategyPlugin';
import { PluginCommitOperation } from './pluginCommitOperation';
import { PluginMergeOperation } from './pluginMergeOperation';
import { PluginObjectActions } from './pluginObjectActions';
import SyncCommitPlugin from './impls/SyncCommitPlugin';
import SyncMergePlugin from './impls/SyncMergePlugin';
import DefaultObjectActionsPlugin from './pluginObjectActions';

export class PluginManager {
    private _repoCreationForm: PluginRepoCreationForm = DefaultRepoCreationFormPlugin;
    private _customObjectRenderers: PluginCustomObjectRenderers = DefaultCustomObjectRenderersPlugin;
    private _loginStrategy: PluginLoginStrategy = RedirectToSSOStrategyPlugin;
    private _commitOperation: PluginCommitOperation = SyncCommitPlugin;
    private _mergeOperation: PluginMergeOperation = SyncMergePlugin;
    private _objectActions: PluginObjectActions = DefaultObjectActionsPlugin;

    overridePluginRepoCreationForm(pluginRepoCreationForm: PluginRepoCreationForm): void {
        this._repoCreationForm = pluginRepoCreationForm;
    }

    get repoCreationForm(): PluginRepoCreationForm {
        return this._repoCreationForm;
    }

    overridePluginCustomObjectRenderers(pluginCustomObjectRenderers: PluginCustomObjectRenderers): void {
        this._customObjectRenderers = pluginCustomObjectRenderers;
    }

    get customObjectRenderers(): PluginCustomObjectRenderers {
        return this._customObjectRenderers;
    }

    overridePluginLoginStrategy(pluginLoginStrategy: PluginLoginStrategy): void {
        this._loginStrategy = pluginLoginStrategy;
    }

    get loginStrategy(): PluginLoginStrategy {
        return this._loginStrategy;
    }

    overridePluginCommitOperation(pluginCommitOperation: PluginCommitOperation): void {
        this._commitOperation = pluginCommitOperation;
    }

    get commitOperation(): PluginCommitOperation {
        return this._commitOperation;
    }

    overridePluginMergeOperation(pluginMergeOperation: PluginMergeOperation): void {
        this._mergeOperation = pluginMergeOperation;
    }

    get mergeOperation(): PluginMergeOperation {
        return this._mergeOperation;
    }

    overridePluginObjectActions(pluginObjectActions: PluginObjectActions): void {
        this._objectActions = pluginObjectActions;
    }

    get objectActions(): PluginObjectActions {
        return this._objectActions;
    }
}
