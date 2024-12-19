/* eslint-disable */
import React, {
    useCallback,
    // useEffect, 
    useContext,
    useState
} from "react";
import Form from "react-bootstrap/Form";
import { ChevronRightIcon } from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import { AlertError, Loading } from "../../../../lib/components/controls";
import AceEditor from "react-ace";

import "ace-builds/src-noconflict/mode-python";
import "ace-builds/src-noconflict/snippets/python";
import "ace-builds/src-noconflict/theme-github";
// import "ace-builds/src-noconflict/ext-language_tools";
import "ace-builds/src-min-noconflict/ext-language_tools";
import { PyodideContext } from "../../../../lib/hooks/pyodideContext";
import { exec } from "child_process";


const cellStyle = {
    marginBottom: '20px',
    border: '1px solid #ddd',
    borderRadius: '4px',
    overflow: 'hidden',
};

const EditorToolbar = ({ onRun, onClear }) => {
    const runButton = (
        <Button type="submit" variant="success" disabled={false}>
            <ChevronRightIcon /> {" "}
            {loading ? "Running..." : "Run"}
        </Button>
    );
    return (
        <div style={toolbarStyle}>
            {/* <button onClick={onRun} style={buttonStyle}>Run</button> */}
            {button}
            <button onClick={onClear} style={buttonStyle}>Clear</button>
            {/* Add more buttons as needed */}
        </div>
    );
};

const toolbarStyle = {
    display: 'flex',
    justifyContent: 'flex-end', // Align buttons to the right
    padding: '5px',
    backgroundColor: '#f5f5f5',
    borderBottom: '1px solid #ddd',
};

const buttonStyle = {
    marginLeft: '5px',
};
const editorContainerStyle = {
    position: 'relative',
};

const initialCode = `import lakefs
from lakefs.client import Client

clt = Client(
    host="https://treeverse.us-east-1.lakefscloud.io",
    username="...",
    password="...",
    verify_ssl=False,
)

repo = lakefs.Repository("isan-v3", client=clt)
repo
import micropip
micropip.list()
import pyodide
from importlib import metadata
"urllib3: " + metadata.version('urllib3') + " requests: " + metadata.version('requests') + " pyodide: " + pyodide.__version__ + " micropip: " + micropip.__version__`;


export const NotebookRenderer = ({ repoId, refId, path, fileExtension }) => {
    const { pyodide, isPyodideLoaded } = useContext(PyodideContext);
    const [error, setError] = useState(null);
    const [code, setCode] = useState(null);
    const [output, setOutput] = useState(null);
    const [loading, setLoading] = useState(false);
    let content;

    const handleSubmit = useCallback((event) => {
        event.preventDefault()
        execCellCode(code, output);
    }, [])

    const handleRun = useCallback(() => {
    }, [])

    const clearOutputs = () => {
        setOutput("");
        setError(null);
    }

    const execCellCode = async (codeContent, currOutput) => {
        clearOutputs();
        if (isPyodideLoaded && pyodide) {
            try {
                console.log("1111111111 codeContent: ", codeContent)
                const result = await pyodide.runPythonAsync(codeContent);
                console.log("222222222 output: ", result)
                if (!result) {
                    setError("No output returned.");
                    return;
                }
                setOutput(currOutput + "\n" + result.toString());
                setError(null);
            } catch (err) {
                setError("Thrown Error: " + err.toString());
            }
        } else {
            setError("Pyodide is not loaded yet.");
        }
    }
    const button = (
        <Button type="submit" variant="success" disabled={false}>
            <ChevronRightIcon /> {" "}
            {loading ? "Running..." : "Run"}
        </Button>
    );
    if (error) {
        content = <AlertError error={error} />;
    } else {
        content = (
            <>
                <div className="object-viewer-sql-results">
                    <h3>Output:</h3>
                    {output}
                </div>
            </>
        );
    }
    return (
        <div>
            <Form onSubmit={handleSubmit}>
                <Form.Group className="mt-2 mb-1" controlId="objectQuery">
                    <h1>Cell </h1>
                    <div>
                        <div style={cellStyle}>
                            <EditorToolbar onRun={() => {
                                // clearOutputs();
                                // execCellCode(content, output);
                            }
                            } onClear={() => {
                                clearOutputs()
                            }} />
                            <div style={editorContainerStyle}>
                                <AceEditor
                                    placeholder="Placeholder Text"
                                    mode="python"
                                    theme="github"
                                    name="blah2"
                                    onLoad={(editor) => { console.log("ace editor loaded") }}
                                    onChange={(newCode) => { setCode(newCode) }}
                                    fontSize={14}
                                    // lineHeight={19}
                                    showPrintMargin={true}
                                    showGutter={true}
                                    highlightActiveLine={true}
                                    enableSnippets={true}
                                    value={code}
                                    setOptions={{
                                        enableBasicAutocompletion: true,
                                        enableLiveAutocompletion: true,
                                        enableSnippets: true,
                                        enableMobileMenu: true,
                                        showLineNumbers: true,
                                        tabSize: 2,
                                    }} />
                            </div>
                        </div>
                    </div>
                </Form.Group>

                <div className="d-flex mb-4">
                    <div className="d-flex flex-fill justify-content-start">
                        {button}
                    </div>

                    <div className="d-flex justify-content-end">
                        <p className="text-muted text-end powered-by">
                            <small>
                                Powered by <a href="https://pyodide.org/en/stable/" target="_blank" rel="noreferrer">pyodide</a>.
                            </small>
                        </p>
                    </div>

                </div>


            </Form>
            <div className="mt-3">
                {content}
            </div>
        </div>
    )
}