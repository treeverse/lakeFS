import { PluginRepoCreationForm } from "./pluginRepoCreationForm";
import DefaultRepoCreationFormPlugin from "./impls/DefaultRepoCreationFormPlugin";
import { PluginCustomObjectRenderers } from "./pluginCustomObjectRenderers";
import DefaultCustomObjectRenderersPlugin from "./impls/DefaultCustomObjectRenderers";
import { PluginLoginMethodSelection } from "./pluginLoginMethodSelection";
import DefaultLoginMethodSelectionPlugin from "./impls/DefaultLoginMethodSelectionPlugin";

export class PluginManager {
    private _repoCreationForm: PluginRepoCreationForm = DefaultRepoCreationFormPlugin;
    private _customObjectRenderers: PluginCustomObjectRenderers = DefaultCustomObjectRenderersPlugin;
    private _loginMethodSelection: PluginLoginMethodSelection = DefaultLoginMethodSelectionPlugin;

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

    overridePluginLoginMethodSelection(pluginLoginMethodSelection: PluginLoginMethodSelection): void {
        this._loginMethodSelection = pluginLoginMethodSelection;
    }

    get loginMethodSelection(): PluginLoginMethodSelection {
        return this._loginMethodSelection;
    }
}