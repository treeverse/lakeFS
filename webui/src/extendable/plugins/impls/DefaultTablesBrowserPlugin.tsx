import React from 'react';
import {PluginTablesBrowser} from '../pluginTablesBrowser';

const TablesPlaceholder: React.FC = () => (
    <p>Tables browsing is an Enterprise feature.</p>
);

class DefaultTablesBrowserPlugin implements PluginTablesBrowser {
    getTablesBrowserComponent(): React.FC {
        return TablesPlaceholder;
    }
}

export default new DefaultTablesBrowserPlugin();
