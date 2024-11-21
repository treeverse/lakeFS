import { PluginAppTabs } from './pluginAppTabs';

export class PluginManager {
    private _appTabs: PluginAppTabs | null = null;

    registerPluginAppTabs(pluginAppTabs: PluginAppTabs) {
        this._appTabs = pluginAppTabs;
    }

    get appTabs(): PluginAppTabs | null {
        return this._appTabs;
    }
}