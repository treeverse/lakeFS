import { createContext, useContext } from 'react';

// simplified: no policy editors.
export const simplified = 'simplified';
// external: allow editing policies; requires lakeFS server to have an
//     external auth service configured.
export const external = 'external';

export const AuthModeContext = createContext(simplified);

export const useSimplified = (() => {
    return useContext(AuthModeContext) === simplified;
});
