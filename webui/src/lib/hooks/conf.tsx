import React, {createContext, useContext} from "react";

import {useAPI} from "./api";
import {setup} from "../api";


export const LoginConfigContext = createContext(null);

export const WithLoginContext = ({children}) => {
    const { response, error, loading } = useAPI(() => setup.getState());
    const value = (error || loading) ? null : response?.login_config;
    return <LoginConfigContext.Provider value={value}>
             {children}
           </LoginConfigContext.Provider>;
};

export const useLoginContext = () => useContext(LoginConfigContext);
