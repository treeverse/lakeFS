import { PluginRepoCreationForm } from "./pluginRepoCreationForm";
import DefaultRepoCreationFormPlugin from "./impls/DefaultRepoCreationFormPlugin";
import { PluginCustomObjectRenderers } from "./pluginCustomObjectRenderers";
import DefaultCustomObjectRenderersPlugin from "./impls/DefaultCustomObjectRenderers";
import { PluginLoginStrategy } from "./pluginLoginStrategy";
import RedirectToSSOStrategyPlugin from "./impls/RedirectToSSOStrategyPlugin";

export class PluginManager {
    private _repoCreationForm: PluginRepoCreationForm = DefaultRepoCreationFormPlugin;
    private _customObjectRenderers: PluginCustomObjectRenderers = DefaultCustomObjectRenderersPlugin;
    private _loginStrategy: PluginLoginStrategy = RedirectToSSOStrategyPlugin;

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
}