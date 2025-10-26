import React, {createContext, useContext, useMemo, useState, useEffect} from "react";
import {auth} from "../api";

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

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    const [status, setStatus] = useState<AuthStatus>(AUTH_STATUS.UNAUTHENTICATED);

    useEffect(() => {
        let isMounted = true;
        const checkAuth = async () => {
            try {
                await auth.getCurrentUser();
                if (isMounted) setStatus(AUTH_STATUS.AUTHENTICATED);
            } catch {
                if (isMounted) setStatus(AUTH_STATUS.UNAUTHENTICATED);
            }
        };
        checkAuth();
        return () => {
            isMounted = false;
        };
    }, []);

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