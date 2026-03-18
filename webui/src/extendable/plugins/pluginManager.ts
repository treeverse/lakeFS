import { PluginRepoCreationForm } from './pluginRepoCreationForm';
import DefaultRepoCreationFormPlugin from './impls/DefaultRepoCreationFormPlugin';
import { PluginCustomObjectRenderers } from './pluginCustomObjectRenderers';
import DefaultCustomObjectRenderersPlugin from './impls/DefaultCustomObjectRenderers';
import { PluginLoginStrategy } from './pluginLoginStrategy';
import RedirectToSSOStrategyPlugin from './impls/RedirectToSSOStrategyPlugin';
import { PluginCommitOperation } from './pluginCommitOperation';
import { PluginMergeOperation } from './pluginMergeOperation';
import SyncCommitPlugin from './impls/SyncCommitPlugin';
import SyncMergePlugin from './impls/SyncMergePlugin';
import { PluginTablesBrowser } from './pluginTablesBrowser';
import DefaultTablesBrowserPlugin from './impls/DefaultTablesBrowserPlugin';
import { PluginCompareBranches } from './pluginCompareBranches';
import DefaultBranchChangesPlugin from './impls/DefaultCompareBranchesPlugin';

export class PluginManager {
    private _repoCreationForm: PluginRepoCreationForm = DefaultRepoCreationFormPlugin;
    private _customObjectRenderers: PluginCustomObjectRenderers = DefaultCustomObjectRenderersPlugin;
    private _loginStrategy: PluginLoginStrategy = RedirectToSSOStrategyPlugin;
    private _commitOperation: PluginCommitOperation = SyncCommitPlugin;
    private _mergeOperation: PluginMergeOperation = SyncMergePlugin;
    private _tablesBrowser: PluginTablesBrowser = DefaultTablesBrowserPlugin;
    private _compareBranches: PluginCompareBranches = DefaultBranchChangesPlugin;

    overridePluginRepoCreationForm(plugin: PluginRepoCreationForm): void {
        this._repoCreationForm = plugin;
    }

    get repoCreationForm(): PluginRepoCreationForm {
        return this._repoCreationForm;
    }

    overridePluginCustomObjectRenderers(plugin: PluginCustomObjectRenderers): void {
        this._customObjectRenderers = plugin;
    }

    get customObjectRenderers(): PluginCustomObjectRenderers {
        return this._customObjectRenderers;
    }

    overridePluginLoginStrategy(plugin: PluginLoginStrategy): void {
        this._loginStrategy = plugin;
    }

    get loginStrategy(): PluginLoginStrategy {
        return this._loginStrategy;
    }

    overridePluginCommitOperation(plugin: PluginCommitOperation): void {
        this._commitOperation = plugin;
    }

    get commitOperation(): PluginCommitOperation {
        return this._commitOperation;
    }

    overridePluginMergeOperation(plugin: PluginMergeOperation): void {
        this._mergeOperation = plugin;
    }

    get mergeOperation(): PluginMergeOperation {
        return this._mergeOperation;
    }

    overridePluginTablesBrowser(plugin: PluginTablesBrowser): void {
        this._tablesBrowser = plugin;
    }

    get tablesBrowser(): PluginTablesBrowser {
        return this._tablesBrowser;
    }

    overridePluginCompareBranches(plugin: PluginCompareBranches): void {
        this._compareBranches = plugin;
    }

    get compareBranches(): PluginCompareBranches {
        return this._compareBranches;
    }
}
