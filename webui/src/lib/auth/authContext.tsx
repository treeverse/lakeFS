import React, { createContext, useContext, useMemo, useState, ReactNode } from "react";

type AuthContextType = {
    status: AuthStatus;
    markAuthenticated: () => void;
    markUnauthenticated: () => void;
};

const AuthContext = createContext<AuthContextType | null>(null);

export const AUTH_STATUS = {
    AUTHENTICATED: "authenticated",
    UNAUTHENTICATED: "unauthenticated",
} as const;

export type AuthStatus = typeof AUTH_STATUS[keyof typeof AUTH_STATUS];

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

    const value = useMemo<AuthContextType>(() => ({
        status,
        markAuthenticated: () => {
            setStatus(AUTH_STATUS.AUTHENTICATED);
            try {
                window.localStorage.setItem(STORAGE_KEY, AUTH_STATUS.AUTHENTICATED);
            }
            catch (e) {
                console.error("[Auth] Failed to persist auth status to localStorage:", e);
            }
        },
        markUnauthenticated: () => {
            setStatus(AUTH_STATUS.UNAUTHENTICATED);
            try {
                window.localStorage.removeItem(STORAGE_KEY);
            }
            catch (e) {
                console.error("[Auth] Failed to clear auth status from localStorage:", e);
            }
        },
    }), [status]);

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextType {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
    return ctx;
}
