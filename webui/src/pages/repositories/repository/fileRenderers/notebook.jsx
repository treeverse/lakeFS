/* eslint-disable */
import React, {
    useCallback,
    useRef,
    useContext,
    useEffect,
    useState
} from "react";
import Form from "react-bootstrap/Form";
import { ChevronRightIcon, InfoIcon,TriangleRightIcon,TrashIcon } from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import { AlertError, Loading } from "../../../../lib/components/controls";
import AceEditor from "react-ace";
import useUser from "../../../../lib/hooks/user";
import { auth } from "../../../../lib/api";

import "ace-builds/src-noconflict/mode-python";
import "ace-builds/src-noconflict/snippets/python";
import "ace-builds/src-noconflict/theme-github";
// import "ace-builds/src-noconflict/ext-language_tools";
import "ace-builds/src-min-noconflict/ext-language_tools";
import { PyodideContext } from "../../../../lib/hooks/pyodideContext";
const snippetReadObjectLabel = "Read Object";
const snippetNumpyArrayLabel = "Numpy Array";
const snippetMicropipListLabel = "pip list";

const initLakeFSPythonPatchTpl = (host, username, password) => {
    return `import lakefs
OriginalClientInit = lakefs.Client.__init__
def CustomClientInit(self, **kwargs):
    OriginalClientInit(self, 
                       host="${host}",
                       username="${username}",
                       password="${password}")
lakefs.Client.__init__ = CustomClientInit`;
};
const initLakeFSPythonPatchCookiAuthTpl = (host, cookie) => {
    return `import lakefs
from lakefs_sdk.client import LakeFSClient
OriginalClientInit = lakefs.Client.__init__
def CustomClientCookieAuth(self, **kwargs):    
    OriginalClientInit(self,
                    host="${host}")
    self._client = LakeFSClient(self._conf, header_name='X-Lakefs-Client',
                                    header_value='python-lakefs/pyodide', cookie='${cookie}')
lakefs.Client.__init__ = CustomClientCookieAuth`;
};
const initLakeFSPythonPatchBrowserDefaultAuthTpl = (host) => {
    return `import lakefs
OriginalClientInit = lakefs.Client.__init__
def CustomClientInit(self, **kwargs):
    OriginalClientInit(self, 
                       host="${host}")
lakefs.Client.__init__ = CustomClientInit`;
};
const cellStyle = {
    marginBottom: '20px',
    border: '1px solid #ddd',
    borderRadius: '4px',
    overflow: 'hidden',
};

const EditorToolbar = ({ onRun, onClear, onSnippet }) => {
    const [loading, setLoading] = useState(false);
    const runButton = (
        <Button type="submit" variant="success" disabled={false}>
            <TriangleRightIcon /> {" "}
            {loading ? "Running..." : " (Ctrl+R)"}
        </Button>
    );
    const clearButton = (
        <Button type="reset" variant="danger" disabled={false}>
            <TrashIcon /> {" "}
            Clear
        </Button>
    );
    const snippets = [
        {
            label: snippetReadObjectLabel,
            icon: <InfoIcon />,
        },
        {
            label: snippetNumpyArrayLabel,
            icon : <InfoIcon />,
        },
        {
            label: snippetMicropipListLabel, 
            icon: <InfoIcon />,
        }
    ]
    return (
        <div style={{display: 'flex'}}>
        <div style={{...toolbarStyle, justifyContent: 'flex-start' }}>
        {snippets.map((snippet, idx) => {
            return (
                <Button key={idx} style={buttonStyle} variant="outline-secondary" onClick={() => {
                    console.log("snippet clicked: ", snippet)
                    if(onSnippet) {
                        onSnippet(snippet);
                    }
                }}>
                    {snippet.icon} {snippet.label}
                </Button>)
        })}
        </div>
        <div style={toolbarStyle}>
            {runButton}
            {clearButton}
            {/* Add more buttons as needed */}
        </div>
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

const microPiplistSnippet = `import micropip
micropip.list()`;
const initialCodeReadObjectTpl = (repoId, refId, path) => {
    return `# Example: Read Current Object
import lakefs
ref = lakefs.repository("${repoId}").branch("${refId}")
obj = ref.object(path="${path}")
obj.stat() 
# print(obj.reader(mode='r').read())
`;
}

export const NotebookRenderer = ({ repoId, refId, path, fileExtension }) => {
    const { pyodide, isPyodideLoaded } = useContext(PyodideContext);
    const [error, setError] = useState(null);
    const initialCodeContent = initialCodeReadObjectTpl(repoId, refId, path);
    const [code, setCode] = useState(initialCodeContent);
    const [output, setOutput] = useState(null);
    const [loading, setLoading] = useState(false);
    const outputRef = useRef([]);
    const formRef = useRef(null);
    const { user } = useUser();
    const [credentials, setCredentials] = useState(null);
    const [authCookie, setAuthCookie] = useState(null);
    const getAuthCookie = () => {
        function getCookie(key) {
            var b = document.cookie.match("(^|;)\\s*" + key + "\\s*=\\s*([^;]+)");
            return b ? b.pop() : "";
        }
        return "internal_auth_session=" + getCookie("internal_auth_session");
    }

    const appendOutput = (msg) => {
        console.log("APPEND OUTPUT CALLED: msg=", msg, "curr output pre append: ", output)
        outputRef.current.push(msg);
    };
    useEffect(() => {
        if (pyodide && user && !authCookie) {
            const cookie = getAuthCookie();
            // no need in cookie, the browser sends the cookies as part of the request, so only the API endpoint is needed
            pyodide.runPython(initLakeFSPythonPatchBrowserDefaultAuthTpl("http://localhost:3000"));
            setAuthCookie(cookie);
            console.log("success setting auth cookie for lakeFS client: ", cookie)
        }
    }, [user, pyodide, isPyodideLoaded])

    useEffect(() => {
        if (pyodide) {
            pyodide.setStdout({ batched: appendOutput });
            console.log("pyodide -> set stdout hook")
        }
    }, [pyodide, isPyodideLoaded]);

    const clearOutputs = () => {
        setOutput("");
        outputRef.current = [];
        setError(null);
    }
    const clearAll = () => {
        setCode("");
        clearOutputs();
    }

    const execCellCode = async (codeContent, currOutput) => {
        clearOutputs();
        if (isPyodideLoaded && pyodide) {
            try {
                const result = await pyodide.runPythonAsync(codeContent);
                let strResultOutput = !result ? "No output returned." : result.toString();
                setOutput(strResultOutput);
                setError(null);
                if (result) {
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
    }, [code, output, error]);

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
                        <div style={{ backgroundColor: 'WhiteSmoke' }}>
                            {renderOutputRef()}
                        </div>
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
            <Form ref={formRef} onSubmit={handleSubmit} onReset={()=>{
                console.log("onReset runs <Form />")
                clearAll()
            }}>
                <Form.Group className="mt-2 mb-1" controlId="objectQuery">
                    <h1>Cell </h1>
                    <div>
                        <div style={cellStyle}>
                            <EditorToolbar 
                            onSnippet={(snippet) => {
                                console.log("to update snippet: ", snippet)
                                clearAll();
                                if(snippet.label === snippetReadObjectLabel) {
                                    setCode(initialCodeReadObjectTpl(repoId, refId, path))
                                } else if(snippet.label === snippetMicropipListLabel) { 
                                    setCode(microPiplistSnippet)
                                }else {
                                    setCode("TODO: " + snippet.label)
                                }
                            }}
                            onRun={() => {
                                // clearOutputs();
                                // execCellCode(content, output);
                            }} 
                            onClear={() => {
                                console.log("onClear runs <EditorToolbar />")
                                // clearOutputs()
                            }} />
                            <div style={editorContainerStyle}>
                                <AceEditor
                                    commands={[
                                        {
                                            name: 'runCommand',
                                            bindKey: { win: 'Ctrl-R', mac: 'Ctrl-R' },
                                            exec: () => {
                                                // simulate form submission otherwise code state var is always empty
                                                if (formRef.current) {
                                                    formRef.current.dispatchEvent(
                                                        new Event('submit', { bubbles: true, cancelable: true })
                                                    );
                                                }

                                            },
                                        },
                                    ]}
                                    placeholder={"import lakefs ..."}
                                    mode="python"
                                    theme="github"
                                    name="blah2"
                                    onLoad={(editor) => { console.log("ace editor loaded") }}
                                    onChange={(newCode) => { setCode(newCode) }}
                                    defaultValue={initialCodeReadObjectTpl(repoId, refId, path)}
                                    fontSize={14}
                                    width="100%"
                                    height="200px"
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