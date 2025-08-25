import React from "react";
import { PluginLoginMethodSelection } from "../pluginLoginMethodSelection";

class DefaultLoginMethodSelectionPlugin implements PluginLoginMethodSelection {
    showLoginMethodSelectionComponent(): boolean {
        return false;
    }

    isLakeFSLoginMethodSelected(): boolean {
        return false;
    }

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    setIsLakeFSLoginMethodSelected(_value: boolean): void {
        // Default implementation does nothing
    }

    renderLoginMethodSelectionComponent(): React.ReactElement | null {
        return null;
    }
}

export default new DefaultLoginMethodSelectionPlugin();