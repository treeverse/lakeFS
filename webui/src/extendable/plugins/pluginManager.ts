import { PluginRepoCreationForm } from "./pluginRepoCreationForm";
import DefaultRepoCreationFormPlugin from "./impls/DefaultRepoCreationFormPlugin";
import { PluginCustomObjectRenderers } from "./pluginCustomObjectRenderers";
import DefaultCustomObjectRenderersPlugin from "./impls/DefaultCustomObjectRenderers";

export class PluginManager {
    private _repoCreationForm: PluginRepoCreationForm = DefaultRepoCreationFormPlugin;
    private _customObjectRenderers: PluginCustomObjectRenderers = DefaultCustomObjectRenderersPlugin;

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
}
