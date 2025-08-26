import React from "react";
import { LoginConfig } from "../../pages/auth/login";

export interface PluginLoginMethodSelection {
    //showLoginMethodSelectionComponent: (loginConfig: LoginConfig) => boolean;
    //isLakeFSLoginMethodSelected: () => boolean;
    //setIsLakeFSLoginMethodSelected: (value: boolean) => void;
    renderLoginMethodSelectionComponent: (loginConfig: LoginConfig) => React.ReactElement | null;
}