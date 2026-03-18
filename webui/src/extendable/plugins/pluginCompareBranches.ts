import React from 'react';

export interface CompareBranchesComponentProps {
    repo: { id: string };
    reference: { id: string; type: string };
    compareReference: { id: string; type: string };
    showActionsBar?: boolean;
    prefix?: string;
    baseSelectURL?: string;
}

export interface PluginCompareBranches {
    getCompareBranchesComponent(): React.FC<CompareBranchesComponentProps> | null;
}
