import React from 'react';
import { BranchChangesComponentProps, PluginCompareBranches } from '../pluginCompareBranches';
import CompareBranches from '../../../lib/components/repository/compareBranches';

class DefaultCompareBranchesPlugin implements PluginCompareBranches {
    getBranchChangesComponent(): React.FC<BranchChangesComponentProps> {
        return CompareBranches;
    }
}

export default new DefaultCompareBranchesPlugin();
