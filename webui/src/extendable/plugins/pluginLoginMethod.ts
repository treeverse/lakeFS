import React from "react";

export interface LoginConfig {
    login_url: string;
    username_ui_placeholder: string;
    password_ui_placeholder: string;
    login_failed_message?: string;
    fallback_login_url?: string;
    fallback_login_label?: string;
    login_cookie_names: string[];
    logout_url: string;
    select_login_method?: boolean;
}

export interface PluginLoginMethod {
    renderLoginMethodComponent: (loginConfig: LoginConfig) => React.ReactElement | null;
}