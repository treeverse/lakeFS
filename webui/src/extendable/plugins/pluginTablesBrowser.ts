import React from 'react';

export interface PluginTablesBrowser {
    getTablesBrowserComponent(): React.FC;
    getTablesActionsComponent(): React.FC | null;
}
