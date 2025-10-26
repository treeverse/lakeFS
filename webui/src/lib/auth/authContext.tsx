import React, { createContext, useContext, useMemo, useState, ReactNode } from "react";

export const AUTH_STATUS = {
    AUTHENTICATED: "authenticated",
    UNAUTHENTICATED: "unauthenticated",
} as const;

export type AuthStatus = typeof AUTH_STATUS[keyof typeof AUTH_STATUS];

type AuthContextType = {
    status: AuthStatus;
    setAuthStatus: (s: AuthStatus) => void;
};

const AuthContext = createContext<AuthContextType | null>(null);

const readInitialStatus = (): AuthStatus =>
    window.localStorage.getItem("user")
        ? AUTH_STATUS.AUTHENTICATED
        : AUTH_STATUS.UNAUTHENTICATED;

export const AuthProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
    const [status, setStatus] = useState<AuthStatus>(() => readInitialStatus());

    const value = useMemo<AuthContextType>(
        () => ({
            status,
            setAuthStatus: setStatus,
        }),
        [status]
    );

    return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
};

export const useAuth = (): AuthContextType => {
    const ctx = useContext(AuthContext);
    if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
    return ctx;
};
