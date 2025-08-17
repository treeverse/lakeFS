import React from "react";
import { PluginLoginMethodSelection } from "../pluginLoginMethodSelection";

class DefaultLoginMethodSelectionPlugin implements PluginLoginMethodSelection {
    renderLoginMethodSelection(): React.ReactElement | null {
        return null;
    }
}

export default new DefaultLoginMethodSelectionPlugin();