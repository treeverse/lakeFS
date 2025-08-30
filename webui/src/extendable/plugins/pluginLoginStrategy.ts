import React from "react";
import { LoginConfig } from "../../pages/auth/login";
import { useRouter } from "../../lib/hooks/router";

export type LoginStrategyResult = {
    element?: React.ReactElement | null;
};

export interface PluginLoginStrategy {
    getLoginStrategy(loginConfig: LoginConfig, router: ReturnType<typeof useRouter>): LoginStrategyResult;
}