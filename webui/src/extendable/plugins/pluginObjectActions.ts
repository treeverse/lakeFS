import { FC, ReactNode } from 'react';

export interface ObjectEntry {
    path: string;
    path_type: 'object' | 'common_prefix';
    checksum?: string;
    size_bytes?: number;
    mtime?: number;
    content_type?: string;
    physical_address?: string;
    metadata?: Record<string, string>;
}

export interface ObjectActionContext {
    repo: { id: string; read_only?: boolean };
    reference: { id: string; type: string };
    entry: ObjectEntry;
    config: {
        pre_sign_support?: boolean;
        pre_sign_support_ui?: boolean;
        blockstore_type?: string;
    };
    onRefresh?: () => void;
}

export interface ObjectAction {
    id: string;
    label: string;
    icon?: ReactNode;
    // Whether this action is available for the given context
    isAvailable: (context: ObjectActionContext) => boolean;
    // Render function - can return a dropdown item or a modal trigger
    render: FC<ObjectActionContext>;
}

export interface PluginObjectActions {
    // Get additional actions to show in the object dropdown menu
    getDropdownActions(): ObjectAction[];

    // Get additional buttons to show in the object viewer toolbar
    getViewerToolbarActions(): ObjectAction[];

    // Get additional options to show in the upload area
    getUploadActions(): ObjectAction[];
}

// Default implementation with no additional actions
class DefaultObjectActionsPlugin implements PluginObjectActions {
    getDropdownActions(): ObjectAction[] {
        return [];
    }

    getViewerToolbarActions(): ObjectAction[] {
        return [];
    }

    getUploadActions(): ObjectAction[] {
        return [];
    }
}

export default new DefaultObjectActionsPlugin();
