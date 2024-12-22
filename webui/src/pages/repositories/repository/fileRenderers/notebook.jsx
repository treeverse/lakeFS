/* eslint-disable */
import React, {
    useCallback,
    useRef,
    useContext,
    useEffect,
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


const cellStyle = {
    marginBottom: '20px',
    border: '1px solid #ddd',
    borderRadius: '4px',
    overflow: 'hidden',
};

const EditorToolbar = ({ onRun, onClear }) => {
    const [loading, setLoading] = useState(false);
    const runButton = (
        <Button type="submit" variant="success" disabled={false}>
            <ChevronRightIcon /> {" "}
            {loading ? "Running..." : "Run"}
        </Button>
    );
    return (
        <div style={toolbarStyle}>
            {/* <button onClick={onRun} style={buttonStyle}>Run</button> */}
            {runButton}
            <button type="reset" onClick={onClear} style={buttonStyle}>Clear</button>
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
    const [stdout, setStdout] = useState(null);
    const [output, setOutput] = useState(null);
    const [loading, setLoading] = useState(false);  
    const outputRef = useRef([]);
    const appendOutput = (msg) => {
        console.log("APPEND OUTPUT CALLED: msg=", msg, "curr output pre append: ", output)
        setStdout(stdout + "\n" + msg);
        setOutput(output + "\n" + msg);
        outputRef.current.push(msg);
    }; 
    useEffect(() => { 
        if(pyodide) {
            pyodide.setStdout({ batched: appendOutput });
            console.log("pyodide -> set stdout hook")
        }
    }, [pyodide, isPyodideLoaded]);

    const clearOutputs = () => {
        setOutput("");
        outputRef.current = [];
        setError(null);
    }

    const execCellCode = async (codeContent, currOutput) => {
        if (isPyodideLoaded && pyodide) {
            try {
                console.log("1111111111 codeContent: ", codeContent)
                const result = await pyodide.runPythonAsync(codeContent);
                console.log("222222222 output: ", result)
                let strResultOutput = !result ? "No output returned." : result.toString();
                setOutput(strResultOutput);
                setError(null);
                if (result){
                    outputRef.current.push(result.toString());
                }
            } catch (err) {
                setError("Pyodide Error: " + err.toString());
            }
        } else {
            setError("Pyodide is not loaded yet.");
        }
    }
    const handleSubmit = useCallback((event) => {
        console.log("handleSubmit form code")
        event.preventDefault()
        execCellCode(code, output);
    }, [code, output, error ]);

    const button = (
        <Button type="submit" variant="success" disabled={false}>
            <ChevronRightIcon /> {" "}
            {loading ? "Running..." : "Run"}
        </Button>
    );
    const renderOutputRef = () => {
        return outputRef.current.map((msg, idx) => { return <div key={idx}>{msg}</div> })
    }
    const renderOutputContent = (currOutput, currError) => {
        if (currError) {
            return <AlertError error={currError} />;
        } else if (currOutput) {
            return (
                <>
                    <div className="object-viewer-sql-results">
                        <h3>Output:</h3>
                        {currOutput}
                        <h3>Stdout:</h3>
                        {stdout}
                        <h3>Combined-Output-Ref:</h3>
                        {renderOutputRef()}
                    </div>
                </>
            );
        } else {
            return (
                <>
                    <div className="object-viewer-sql-results">
                        <h3>Clean No Output</h3>
                    </div>
                </>
            );
        }
    }
    return (
        <div>
            <Form onSubmit={handleSubmit} onReset={clearOutputs}>
                <Form.Group className="mt-2 mb-1" controlId="objectQuery">
                    <h1>Cell </h1>
                    <div>
                        <div style={cellStyle}>
                            <EditorToolbar onRun={() => {
                                // clearOutputs();
                                // execCellCode(content, output);
                            }
                            } onClear={() => {
                                console.log("onClear runs <EditorToolbar />")
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
                    {/* <div className="d-flex flex-fill justify-content-start">
                        {button}
                    </div> */}

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
                {renderOutputContent(output, error)}
            </div>
        </div>
    )
}