import React from "react";
import { PluginLoginMethodSelection, LoginConfig } from "../pluginLoginMethodSelection";

class DefaultLoginMethodSelectionPlugin implements PluginLoginMethodSelection {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    renderMethodSelection(_loginConfig: LoginConfig | undefined): React.ReactElement | null {
        return null;
    }
}

export default new DefaultLoginMethodSelectionPlugin();