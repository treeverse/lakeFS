import React, {createContext, useContext, useMemo} from "react";

import {useAPI} from "./api";
import {setup} from "../api";


export const LoginConfigContext = createContext(null);

export const WithLoginConfigContext = ({children}) => {
    const { response, error, loading } = useAPI(() => setup.getState());
    const lc = useMemo(() => (error || loading) ? null : response?.login_config, [response]);
    return <LoginConfigContext.Provider value={lc}>
             {children}
           </LoginConfigContext.Provider>;
};

export const useLoginConfigContext = () => useContext(LoginConfigContext);
