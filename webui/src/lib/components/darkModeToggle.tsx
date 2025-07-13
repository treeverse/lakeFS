import React, { FC, useContext } from "react";
import "@theme-toggles/react/css/Classic.css";
import { Classic } from "@theme-toggles/react";
import { AppActionType, AppContext } from "../hooks/appContext";

const DarkModeToggle: FC = () => {
    const { state, dispatch } = useContext(AppContext);

    const toggleDarkMode = (toggled: boolean) => {
        dispatch({
            type: AppActionType.setDarkMode,
            value: toggled,
        });
    };

    return (
        <Classic
            toggled={state.settings.darkMode}
            onToggle={toggleDarkMode}
            duration={750}
            placeholder=""
            style={{
                color: 'white',
                fontSize: 'var(--spacing-xl)',
                marginRight: 'var(--spacing-md)',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
            }}
        />
    );
};

export default DarkModeToggle;
