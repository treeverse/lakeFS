import React, {useCallback, useState} from 'react';
import Editor  from 'react-simple-code-editor';
import Prism from 'prismjs';
import 'prismjs/components/prism-sql';
import "../../../../styles/ghsyntax.css";


export const SQLEditor = ({ initialValue, onChange, onRun }) => {
    const isMac = window.navigator.platform.toLowerCase().startsWith('mac');
    const modKey = isMac ? 'Meta' : 'Control';
    const [modPressed, setModPressed] = useState(false);
    const [code, setCode] = React.useState(initialValue);
    const changeHandler = useCallback((code) => {
        setCode(code)
        if (onChange)
            onChange(code)
    }, [setCode, onChange])
    return (
        <Editor
            value={code}
            onValueChange={changeHandler}
            highlight={code => Prism.highlight(code, Prism.languages.sql, 'sql')}
            padding={10}
            className="syntax-editor"
            onKeyDown={(e) => {
                console.log(`${e.key} ${modPressed}`)
                if (e.key === modKey) {
                    setModPressed(true);
                } else if (e.key === 'Enter' && modPressed) {
                    e.preventDefault();
                    e.stopPropagation();
                    onRun();
                }
            }}
            onKeyUp={(e) => {
                if (e.key === modKey) {
                    setModPressed(false);
                }
            }}
        />
    );
}