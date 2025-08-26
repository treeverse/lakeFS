import React from "react";
import { PluginLoginMethodSelection } from "../pluginLoginMethodSelection";
//import { LoginConfig } from "../../pages/auth/login";

class DefaultLoginMethodSelectionPlugin implements PluginLoginMethodSelection {
    // showLoginMethodSelectionComponent(): boolean {
    //     return false;
    // }
    //
    // isLakeFSLoginMethodSelected(): boolean {
    //     return false;
    // }
    //
    // setIsLakeFSLoginMethodSelected(): void {
    //     // Default implementation does nothing
    // }

    renderLoginMethodSelectionComponent(): React.ReactElement | null {
        // if (loginConfig.login_url) {
        //     const RedirectComponent = () => {
        //         window.location.href = loginConfig.login_url;
        //         return <div>Redirecting...</div>;
        //     };
        //     return <RedirectComponent />;
        // }
        // return null;

        return null;
    }
}

export default new DefaultLoginMethodSelectionPlugin();