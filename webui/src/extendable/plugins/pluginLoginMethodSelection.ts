import React from "react";
import { LoginConfig } from "../../pages/auth/login";

export interface PluginLoginMethodSelection {
    renderLoginMethodSelection: (loginConfig: LoginConfig) => React.ReactElement | null;
}