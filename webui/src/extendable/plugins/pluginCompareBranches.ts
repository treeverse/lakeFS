import React from 'react';

export interface BranchChangesComponentProps {
    repo: { id: string };
    reference: { id: string; type: string };
    compareReference: { id: string; type: string };
    showActionsBar?: boolean;
    prefix?: string;
    baseSelectURL?: string;
}

export interface PluginCompareBranches {
    getBranchChangesComponent(): React.FC<BranchChangesComponentProps> | null;
}
