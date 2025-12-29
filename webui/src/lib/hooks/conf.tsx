import React, { ReactNode, createContext, useContext, useMemo } from 'react';

import { useAPI } from './api';
import { setup } from '../api';

type LoginConfig = {
    RBAC: string | null;
    FallbackLoginLabel: string | null;
    FallbackLoginUrl: string | null;
    LoginCookieNames: string[];
    LoginFailedMessage: string | null;
    LoginUrl: string;
    LogoutUrl: string;
};

const initValue = {
    RBAC: null,
    FallbackLoginLabel: null,
    FallbackLoginUrl: null,
    LoginCookieNames: [],
    LoginFailedMessage: null,
    LoginUrl: '',
    LogoutUrl: '',
};

export const LoginConfigContext = createContext<LoginConfig>(initValue);

export const WithLoginConfigContext = ({ children }: { children: ReactNode }) => {
    const { response, error, loading } = useAPI(() => setup.getState());
    // this will be fixed when we have proper types for the API
    const lc = useMemo(
        () =>
            error || loading || !('login_config' in (response as Record<string, unknown>))
                ? initValue
                : ((response as Record<string, unknown>).login_config as LoginConfig) || {},
        [response],
    );
    return <LoginConfigContext.Provider value={lc}>{children}</LoginConfigContext.Provider>;
};

export const useLoginConfigContext = () => useContext(LoginConfigContext);
