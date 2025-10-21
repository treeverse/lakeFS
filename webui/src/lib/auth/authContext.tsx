import React, { createContext, useContext, useMemo, useState, ReactNode } from "react";
import { AUTH_STATUS, type AuthStatus } from "./status";

type AuthContextShape = {
    status: AuthStatus;
    markAuthenticated: () => void;
    markUnauthenticated: () => void;
};

const AuthContext = createContext<AuthContextShape | null>(null);
const STORAGE_KEY = "lakefs:ui:auth:status";

function readPersistedStatus(): AuthStatus {
    try {
        const v = window.localStorage.getItem(STORAGE_KEY);
        return v === AUTH_STATUS.AUTHENTICATED ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED;
    } catch {
        return AUTH_STATUS.UNAUTHENTICATED;
    }
}

export function AuthProvider({ children }: { children: ReactNode }) {
    const [status, setStatus] = useState<AuthStatus>(() => readPersistedStatus());

    const value = useMemo<AuthContextShape>(() => ({
        status,
        markAuthenticated: () => {
            setStatus(AUTH_STATUS.AUTHENTICATED);
            try { window.localStorage.setItem(STORAGE_KEY, AUTH_STATUS.AUTHENTICATED); } catch { return }
        },
        markUnauthenticated: () => {
            setStatus(AUTH_STATUS.UNAUTHENTICATED);
            try { window.localStorage.removeItem(STORAGE_KEY); } catch { return }
        },
    }), [status]);

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextShape {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
    return ctx;
}
