import React from 'react';

export interface PluginTablesBrowser {
    getTablesBrowserComponent(): React.FC | null;
}
