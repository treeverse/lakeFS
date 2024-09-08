import React, { createContext, useReducer } from "react";

type AppContextType = {
    settings: AppContext;
};

type AppContext = {
    darkMode: boolean;
};

const localStorageKeys = {
    darkMode: 'darkMode',
};

enum AppActionType {
    setDarkMode = 'setDarkMode',
}

interface Action {
    type: AppActionType;
    value: boolean;
}

const initialLocalSettings: AppContext = {
    darkMode: window.localStorage.getItem(localStorageKeys.darkMode) === String(true),
};

const initialAppContext: AppContextType = {
    settings: initialLocalSettings,
};

const appContextReducer = (state: AppContextType, action: Action) => {
    switch (action.type) {
        case AppActionType.setDarkMode:
            window.localStorage.setItem(localStorageKeys.darkMode, String(action.value));
            return {...state, settings: {...state.settings, darkMode: action.value}};
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
