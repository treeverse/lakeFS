import {LoginStrategyResult, PluginLoginStrategy} from "../pluginLoginStrategy";
import { LoginConfig } from "../../pages/auth/login";

class RedirectToSSOStrategyPlugin implements PluginLoginStrategy {
    // Auto-redirect to the configured SSO login_url when available.
    getLoginStrategy(loginConfig: LoginConfig): LoginStrategyResult {
        if (loginConfig.login_url) {
            window.location.href = loginConfig.login_url;
            return { type: 'redirected' };
        }
        return { type: 'none' };
    }
}

export default new RedirectToSSOStrategyPlugin();