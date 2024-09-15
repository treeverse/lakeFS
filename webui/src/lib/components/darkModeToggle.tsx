import React, {FC, useContext} from "react";
import {DarkModeSwitch} from "react-toggle-dark-mode";
import {AppActionType, AppContext} from "../hooks/appContext";

const DarkModeToggle: FC = () => {
    const {state, dispatch} = useContext(AppContext);

    const toggleDarkMode = (isOn: boolean) => {
        dispatch({
            type: AppActionType.setDarkMode,
            value: isOn,
        });
    };

    return (
        <DarkModeSwitch
            style={{marginRight: '2rem'}}
            checked={state.settings.darkMode}
            onChange={toggleDarkMode}
            size={28}
            sunColor={"white"}
        />
    );
};

export default DarkModeToggle;
