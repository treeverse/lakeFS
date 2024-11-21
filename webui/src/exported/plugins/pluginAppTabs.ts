import React from "react";

export interface AppTab {
    id: string;
    name: string;
    component: React.FC;
}

export interface PluginAppTabs {
    tabs: AppTab[];
}
