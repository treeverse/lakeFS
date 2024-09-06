import React, {useState} from "react";
import {DarkModeSwitch} from "react-toggle-dark-mode";

const keyIsDarkMode = "is_dark_mode";

const DarkModeToggle = () => {
    const [isDarkMode, setDarkMode] = useState(
        window.localStorage.getItem(keyIsDarkMode) === String(true)
    );

    const toggleDarkMode = (isOn) => {
        // TODO: use Redux or similar when introduced into this project, instead of using localStorage listeners.
        window.localStorage.setItem(keyIsDarkMode, String(isOn));
        window.dispatchEvent(new StorageEvent("storage", {key: keyIsDarkMode}));
        setDarkMode(isOn)
    };

    return (
        <DarkModeSwitch
            style={{ marginRight: '2rem' }}
            checked={isDarkMode}
            onChange={toggleDarkMode}
            size={28}
            sunColor={"white"}
        />
    );
};

export default DarkModeToggle;
