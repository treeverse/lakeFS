import React from "react";
import { PluginLoginMethod, LoginConfig } from "../pluginLoginMethod";

class DefaultLoginMethodPlugin implements PluginLoginMethod {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    renderLoginMethodComponent(_loginConfig: LoginConfig): React.ReactElement | null {
        return null;
    }
}

export default new DefaultLoginMethodPlugin();