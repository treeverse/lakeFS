import React from "react";
import { LoginConfig } from "../../pages/auth/login";

export type LoginStrategyResult = {
    element?: React.ReactElement | null;
};

export interface PluginLoginStrategy {
    getLoginStrategy(loginConfig: LoginConfig): LoginStrategyResult;
}