import React, { createContext, useReducer } from "react";

type AppContextType = {
    settings: AppContext;
};

type AppContext = {
    darkMode: boolean;
    sqlQueryTemplates: { [key: string]: string | undefined };
};

const localStorageKeys = {
    darkMode: 'darkMode',
    sqlQueryTemplates: 'sqlQueryTemplates',
};

enum AppActionType {
    setDarkMode = 'setDarkMode',
    setSqlQueryTemplates = 'setSqlQueryTemplates',
}

interface Action {
    type: AppActionType;
    value: boolean | { [key: string]: string | undefined };
}

const initialLocalSettings: AppContext = {
    darkMode: window.localStorage.getItem(localStorageKeys.darkMode) === String(true),
    sqlQueryTemplates: JSON.parse(window.localStorage.getItem(localStorageKeys.sqlQueryTemplates) || '{}'),
};

const initialAppContext: AppContextType = {
    settings: initialLocalSettings,
};

const appContextReducer = (state: AppContextType, action: Action) => {
    switch (action.type) {
        case AppActionType.setDarkMode:
            window.localStorage.setItem(localStorageKeys.darkMode, String(action.value));
            return {...state, settings: {...state.settings, darkMode: action.value}};
        case AppActionType.setSqlQueryTemplates:
            window.localStorage.setItem(localStorageKeys.sqlQueryTemplates, JSON.stringify(action.value));
            return {...state, settings: {...state.settings, sqlQueryTemplates: action.value as { [key: string]: string | undefined }}};
        default:
            return state;
    }
}

type ContextType = {
    state: AppContextType,
    dispatch: React.Dispatch<Action>,
};

const AppContext = createContext<ContextType>({
    state: initialAppContext,
    dispatch: () => null
});

// @ts-expect-error - it doesn't like the "children" prop
const WithAppContext: React.FC = ({children}) => {
    const [state, dispatch] = useReducer(appContextReducer, initialAppContext);

    return (
        <AppContext.Provider value={{state, dispatch}}>
            {children}
        </AppContext.Provider>
    )
}

export { WithAppContext, AppContext, AppActionType };
