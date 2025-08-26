import React from "react";
import { LoginConfig } from "../../pages/auth/login";

export type LoginStrategyResult =
    | { type: 'render'; element: React.ReactElement }
    | { type: 'redirected' }
    | { type: 'none' };

export interface PluginLoginStrategy {
    getLoginStrategy(loginConfig: LoginConfig): LoginStrategyResult;
}