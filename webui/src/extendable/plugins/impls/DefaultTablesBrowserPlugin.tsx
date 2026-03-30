import React from 'react';
import { PluginTablesBrowser } from '../pluginTablesBrowser';
import { TablesEnterpriseInfo } from '../../../lib/components/repository/tablesEnterpriseInfo';

class DefaultTablesBrowserPlugin implements PluginTablesBrowser {
    getTablesBrowserComponent(): React.FC {
        return TablesEnterpriseInfo;
    }

    getTablesActionsComponent(): React.FC | null {
        return null;
    }

    getTablesPrefix(): null {
        return null;
    }
}

export default new DefaultTablesBrowserPlugin();
