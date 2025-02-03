import { PluginRepoCreationForm } from "./pluginRepoCreationForm";

export class PluginManager {
    private _repoCreationForm: PluginRepoCreationForm | null = null;

    registerPluginRepoCreationForm(pluginRepoCreationForm: PluginRepoCreationForm): void {
        this._repoCreationForm = pluginRepoCreationForm;
    }

    get repoCreationForm(): PluginRepoCreationForm | null {
        return this._repoCreationForm;
    }
}
