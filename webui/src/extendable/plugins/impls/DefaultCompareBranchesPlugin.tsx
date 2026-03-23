import React from 'react';
import { CompareBranchesComponentProps, PluginCompareBranches } from '../pluginCompareBranches';
import CompareBranches from '../../../lib/components/repository/compareBranches';

class DefaultCompareBranchesPlugin implements PluginCompareBranches {
    getCompareBranchesComponent(): React.FC<CompareBranchesComponentProps> {
        return CompareBranches;
    }
}

export default new DefaultCompareBranchesPlugin();
