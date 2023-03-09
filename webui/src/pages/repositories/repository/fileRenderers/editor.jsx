import React, {useCallback} from 'react';
import Editor  from 'react-simple-code-editor';
import Prism from 'prismjs';
import 'prismjs/components/prism-sql';
import "../../../../styles/ghsyntax.css";


export const SQLEditor = ({ initialValue, onChange }) => {
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
        />
    );
}