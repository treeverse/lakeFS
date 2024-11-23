import React from 'react';
import { createRoot } from 'react-dom/client';

// styles
import 'bootstrap/dist/css/bootstrap.css';
import './styles/globals.css';

// app and plugins system
import LakeFSApp from "./exported/lakefsApp";
import { PluginManager } from "./exported/plugins/pluginManager";

const pluginManager = new PluginManager();
// Register a plugin
const appTabsPlugin = {
    tabs: [
        {
            id: 'extra-tab',
            name: 'Extra Tab',
            component: () => <div>Extra Tab Content</div>,
        },
    ],
};
pluginManager.registerPluginAppTabs(appTabsPlugin);

const container = document.getElementById('root');
if (!container) throw new Error("Failed to find root element!");

const root = createRoot(container);
root.render(<LakeFSApp pluginManager={pluginManager}/>);
