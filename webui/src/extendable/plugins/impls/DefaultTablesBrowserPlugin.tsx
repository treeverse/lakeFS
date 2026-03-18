import React from 'react';
import { PluginTablesBrowser } from '../pluginTablesBrowser';
import { TablesEnterpriseInfo } from '../../../lib/components/repository/tablesEnterpriseInfo';

class DefaultTablesBrowserPlugin implements PluginTablesBrowser {
    getTablesBrowserComponent(): React.FC {
        return TablesEnterpriseInfo;
    }
}

export default new DefaultTablesBrowserPlugin();
