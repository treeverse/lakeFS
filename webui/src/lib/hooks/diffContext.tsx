import React, { createContext, useReducer } from "react";

type DiffContextType = {
    results: object[] | null;
    loading: boolean;
    error: string | null;
    nextPage: object | null;
};

enum DiffActionType {
    setResults = 'setResults',
}

interface Action {
    type: DiffActionType;
    value: DiffContextType;
}

const initialDiffContext: DiffContextType = {
    results: [],
    loading: false,
    error: null,
    nextPage: null,
};

const diffContextReducer = (state: DiffContextType, action: Action) => {
    switch (action.type) {
        case DiffActionType.setResults:
            return {...state, ...action.value};
        default:
            return state;
    }
}

type ContextType = {
    state: DiffContextType,
    dispatch: React.Dispatch<Action>,
};

const DiffContext = createContext<ContextType>({
    state: initialDiffContext,
    dispatch: () => null
});

// @ts-expect-error - it doesn't like the "children" prop
const WithDiffContext: React.FC = ({children}) => {
    const [state, dispatch] = useReducer(diffContextReducer, initialDiffContext);

    return (
        <DiffContext.Provider value={{state, dispatch}}>
            {children}
        </DiffContext.Provider>
    )
}

export { WithDiffContext, DiffContext, DiffActionType };
