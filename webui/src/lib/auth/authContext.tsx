import React, { createContext, useContext, useMemo, useState, ReactNode } from "react";

type AuthContextType = {
    status: AuthStatus;
    setAuthStatus: (s: AuthStatus) => void;
};

const AuthContext = createContext<AuthContextType | null>(null);

export const AUTH_STATUS = {
    AUTHENTICATED: "authenticated",
    UNAUTHENTICATED: "unauthenticated",
} as const;

export type AuthStatus = typeof AUTH_STATUS[keyof typeof AUTH_STATUS];

function readInitialStatus(): AuthStatus {
    return window.localStorage.getItem('user') ? AUTH_STATUS.AUTHENTICATED : AUTH_STATUS.UNAUTHENTICATED;
}

export function AuthProvider({ children }: { children: ReactNode }) {
    const [status, setStatus] = useState<AuthStatus>(() => readInitialStatus());

    const value = useMemo<AuthContextType>(() => ({
        status,
        setAuthStatus: setStatus,
    }), [status]);

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth(): AuthContextType {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
    return ctx;
}
