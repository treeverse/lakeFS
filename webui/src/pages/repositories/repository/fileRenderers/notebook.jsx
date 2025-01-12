/* eslint-disable */
import React, {
    useCallback,
    useRef,
    useContext,
    useEffect,
    useState
} from "react";
import { FileType } from "./types";
import Form from "react-bootstrap/Form";
import Dropdown from 'react-bootstrap/Dropdown';
import DropdownButton from 'react-bootstrap/DropdownButton';
import { ChevronRightIcon, InfoIcon, TriangleRightIcon, TrashIcon } from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import { AlertError, Loading } from "../../../../lib/components/controls";
import AceEditor from "react-ace";
import useUser from "../../../../lib/hooks/user";
import { auth } from "../../../../lib/api";
import { useAPI } from "../../../../lib/hooks/api";
import { objects, qs } from "../../../../lib/api";
import { TextDownloader } from "./simple";
import "ace-builds/src-noconflict/mode-python";
import "ace-builds/src-noconflict/snippets/python";
import "ace-builds/src-noconflict/theme-github";
// import "ace-builds/src-noconflict/ext-language_tools";
import "ace-builds/src-min-noconflict/ext-language_tools";
import { PyodideContext } from "../../../../lib/hooks/pyodideContext";
import { guessType } from ".";
import { DuckDBRenderer } from "./data";
const snippetStatObjectLabel = "Stat current object";
const snippetReadObjectLabel = "Read current (Text) object";
const snippetNumpyMatplotlibLabel = "Numpy + Matplotlib";
const snippetMicropipListLabel = "pip list";
const snippetReadCurrentParquetLabel = ".parquet Read (pandas)";
const snippetReadCurrentImageLabel = "Read current image";
const snippetMagicLoadLocal = "Load Python script file";

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

const initRuntimePatches = () => {
    return `import matplotlib.pyplot as plt
import io
import base64
from js import document

def show_b64_image(img_base64):
  # Create an HTML image element
  img_html = f'<img id="image-output" src="data:image/png;base64,{img_base64}" />'

  # Insert the image into the specified HTML element
  plot_container = document.getElementById('image-container')
  plot_container.innerHTML = img_html

def show_plot():
  # Save the plot to a BytesIO object
  buf = io.BytesIO()
  plt.savefig(buf, format='png')
  buf.seek(0)
  # Encode the image to base64
  img_base64 = base64.b64encode(buf.read()).decode('utf-8')
  show_b64_image(img_base64)
`;
}

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

const EditorToolbar = ({ onRun, onClear, onSnippet, loading, loadingMode, defaultSnippetLabel }) => {
    const snippets = [
        {
            label: snippetStatObjectLabel,
            icon: <InfoIcon />,
        },
        {
            label: snippetReadObjectLabel,
            icon: <InfoIcon />,
        },
        {
            label: snippetMicropipListLabel,
            icon: <InfoIcon />,
        },
        {
            label: snippetReadCurrentParquetLabel,
            icon: <InfoIcon />,
        },
        {
            label: snippetNumpyMatplotlibLabel,
            icon: <InfoIcon />,
        },
        {
            label: snippetReadCurrentImageLabel,
            icon: <InfoIcon />,
        },
        {
            label: snippetMagicLoadLocal,
            icon: <InfoIcon />,
        }
    ];
    const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
    const ctrlLabelKeyBinding = isMac ? "Cmd" : "Ctrl";
    const [selectedSnippet, setSelectedSnippet] = useState(snippets.find(snippet => snippet.label === defaultSnippetLabel) || null);
    const runButton = (
        <Button type="submit" variant="success" style={{ margin: '3px' }} disabled={loading}>
            <ChevronRightIcon /> {" "}
            {loading ? loadingMode : `Run (${ctrlLabelKeyBinding}+Enter)`}
        </Button>
    );
    const clearButton = (
        <Button type="reset" variant="danger" style={{ margin: '3px' }} disabled={false} onClick={() => setSelectedSnippet(null)}>
            <TrashIcon /> {" "}
            Clear
        </Button>
    );
    useEffect(() => {
        if(snippets.find(snippet => snippet.label === defaultSnippetLabel)){
            onSnippet(snippets.find(snippet => snippet.label === defaultSnippetLabel));
        }
    }, [defaultSnippetLabel]);
    return (
        <div style={{ display: 'flex', justifyContent: 'space-between', backgroundColor: '#f5f5f5', padding: '5px' }}>
            <div style={{ ...toolbarStyle, justifyContent: 'flex-start' }}>
                <DropdownButton 
                id="dropdown-basic-button" 
                title={selectedSnippet ? "Example: " + selectedSnippet.label : "Examples (Python)"}
                >
                    {snippets.map((snippet, idx) => {
                        return (
                            <Dropdown.Item key={idx} style={buttonStyle} variant="secondary" onClick={() => {
                                setSelectedSnippet(snippet);
                                if (onSnippet) {
                                    onSnippet(snippet);
                                }
                            }}>
                                {snippet.icon} {snippet.label}
                            </Dropdown.Item>)
                    })}
                </DropdownButton>
            </div>
            <div style={toolbarStyle}>
                {clearButton}
                {runButton}
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

const readCurrentImageSnippetTpl = (repoId, refId, path, fileExtension) => {
    let warn = ""
    if (guessType(null, fileExtension) !== FileType.IMAGE) {
        warn = `# !!!NOTICE!!!: Current file ${fileExtension} extension is not image, change path to an image object\n`;
    }
    return warn + `# Example: Render current image (NOTE: CHANGE PATH IF OBJECT NOT AN IMAGE)

import base64

ref = lakefs.repository("${repoId}").branch("${refId}")
obj = ref.object(path="images/axolotl.png")
img = obj.reader(mode='rb')
data = img.read()
img_b64 = base64.b64encode(data).decode('utf-8')

# NOTE: show_b64_image is a runtime utility func (for easy display there are many ways displaying images)
show_b64_image(img_b64)`;
}
const microPiplistSnippet = `import micropip
micropip.list()`;

const readCurrParquetTpl = (repoId, refId, path, fileExtension) => {
    let warn = "";
    if (guessType(null, fileExtension) != FileType.PARQUET) {
        warn = `# !!!NOTICE!!!: Current file extension is not parquet, change path to a parquet object\n`;
    }
    return warn + `# Example: Read Current Object (NOTE: CHANGE PATH IF OBJECT NOT A PARQUET)
import pandas as pd
ref = lakefs.repository("${repoId}").branch("${refId}")
obj = ref.object(path="${path}")
df = pd.read_parquet(obj.reader(mode='rb'))
df.head()`;
}
const statsCurrentObjectTpl = (repoId, refId, path) => {
    return `# Example: Read Current Object
import lakefs
ref = lakefs.repository("${repoId}").branch("${refId}")
obj = ref.object(path="${path}")
print(obj.stat())`;
}
const initialCodeReadObjectTpl = (repoId, refId, path) => {
    return `# Example: Read Current Object
import lakefs
ref = lakefs.repository("${repoId}").branch("${refId}")
obj = ref.object(path = "${path}")
print("--- object stats ---")
print(obj.stat())
print("--- read object ---")
try:
    print(obj.reader(mode = 'r').read())
except UnicodeDecodeError:
  # if reading iamge / parquet etc obj.reader(mode = 'rb')
    print("Invalid UTF-8, try binary 'mode=rb'")
`;
}
const magicLoadLocalScriptSnippet = () => {
    return `# load local python script file(s)
# %% load_local file1.py file2.py file3.py
%%load_local hello_world.py
# assuming function say_hello is defined in hello_world.py
say_hello("axolotl")
# assuming calculate_sum(a,b) is defined in hello_world.py
result = calculate_sum(1,2)
print("Sum" ,result)`;
}
const numpyMatplotlibSnippet = () => {
    return `
import numpy as np
import matplotlib.pyplot as plt

# Generate data
x = np.linspace(0, 2 * np.pi, 100)  # 100 values from 0 to 2π
y = np.sin(x)  # sine of each x value

# Create the plot
plt.plot(x, y)
plt.xlabel('x')
plt.ylabel('y')
plt.title('Plot Title')

# show plot as image (runtime utility function)
# TODO: the default interactive plt.show() also works but weird.
show_plot()`;
};

const parseEditorContent = (inputText) => {
    // Split the input text into individual lines
    const lines = inputText.split('\n');

    // Initialize arrays to hold magic commands and code lines
    const magicCommands = [];
    const codeLines = [];

    // Iterate over each line to classify it
    lines.forEach(line => {
        const trimmedLine = line.trim();
        if (trimmedLine.startsWith('%%')) {
            const cmd = parseMagicCommand(trimmedLine);
            if (cmd) {
                magicCommands.push(cmd);
            }
            //magicCommands.push(trimmedLine.slice(2).trim());
        } else {
            // Add the line to the codeLines array
            codeLines.push(line);
        }
    });

    // Join the code lines back into a single string
    const code = codeLines.join('\n');

    // Return an object containing both the magic commands and the code
    return { magicCommands, codeContent: code };
}
/**
 * Parses a magic command line and extracts the function name and arguments.
 * @param {string} line - The magic command line to parse.
 * @returns {Object|null} An object containing 'funcName' and 'args' if valid, otherwise null.
 */
const parseMagicCommand = (line) => {
    // Trim leading and trailing whitespace
    const trimmedLine = line.trim();

    // Regular expression to match the magic command pattern
    const magicCommandPattern = /^%%(\w+)\s*(.*)$/;

    // Test if the line matches the magic command pattern
    const match = trimmedLine.match(magicCommandPattern);
    if (match) {
        // Extract function name
        const funcName = match[1];

        // Extract arguments string
        const argsString = match[2].trim();

        // Split arguments by comma and trim whitespace
        const args = argsString ? argsString.split(',').map(arg => arg.trim()) : [];

        return { funcName, args };
    } else {
        // Return null if the line does not match the expected pattern
        return null;
    }
}
export const NotebookRenderer = ({ repoId, refId, path, fileExtension }) => {
    const { pyodide, isPyodideLoaded } = useContext(PyodideContext);
    const [error, setError] = useState(null);
    const initialCodeContent = initialCodeReadObjectTpl(repoId, refId, path);
    const [code, setCode] = useState("");
    const [output, setOutput] = useState(null);
    const [loading, setLoading] = useState(false);
    const outputRef = useRef([]);
    const formRef = useRef(null);
    const { user } = useUser();
    const [credentials, setCredentials] = useState(null);
    const [authCookie, setAuthCookie] = useState(null);

    const handleKeyDown = (event) => {
        if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
            // Prevent default behavior if necessary
            event.preventDefault();
            // Execute the desired function
            if (formRef.current) {
                formRef.current.dispatchEvent(
                    new Event('submit', { bubbles: true, cancelable: true })
                );
            }
        }
    };

    useEffect(() => {
        // Add event listener when the component mounts
        window.addEventListener('keydown', handleKeyDown);
        // Remove event listener when the component unmounts
        return () => {
            window.removeEventListener('keydown', handleKeyDown);
        };
    }, [formRef]);

    const getAuthCookie = () => {
        const key = "internal_auth_session";
        let b = document.cookie.match("(^|;)\\s*" + key + "\\s*=\\s*([^;]+)");
        const cookieValue = b ? b.pop() : "";
        return "internal_auth_session=" + cookieValue;
    }

    const appendOutput = (msg) => {
        outputRef.current.push(msg);
    };
    useEffect(() => {
        if (pyodide && user && !authCookie) {
            const cookie = getAuthCookie();
            // no need in cookie, the browser sends the cookies as part of the request, so only the API endpoint is needed
            const lakeFSHost = `${window.location.protocol}//${window.location.host}`
            pyodide.runPythonAsync(initLakeFSPythonPatchBrowserDefaultAuthTpl(lakeFSHost)).then(() => {
                setAuthCookie(cookie);
                pyodide.runPythonAsync(initRuntimePatches()).then(() => {
                    console.log("success initiating runtime")
                }).catch(setError);
            }).catch(setError);
        }
    }, [user, pyodide, isPyodideLoaded])

    useEffect(() => {
        if (pyodide) {
            pyodide.setStdout({ batched: appendOutput });
        }
    }, [pyodide, isPyodideLoaded]);

    const clearImageContainer = () => {
        let parentElement = document.getElementById('image-container');
        let imgElement = document.getElementById('image-output');
        if (parentElement && imgElement) {
            parentElement.removeChild(imgElement);
        }
    }
    const clearOutputs = () => {
        clearImageContainer();
        setOutput("");
        outputRef.current = [];
        setError(null);
    }

    const clearAll = () => {
        setCode("");
        clearOutputs();
    }

    const handleMagic = async ({ funcName, args }, repo, ref, presign) => {
        switch (funcName) {
            case "load_local":
                let results = {};
                if (!args || args.length === 0) {
                    throw new Error("load_local: missing argument(s): file path(s)");
                }
                // for each file path in args, load the file and store it in a variable
                for (const filePath of args) {
                    // check if the file extension (path) ends with FileType.IPYNB, error if not 
                    // extract the extentions from the file path
                    const extension = filePath.split('.').pop();
                    if (guessType(null, extension) !== FileType.IPYNB && extension !== 'py') {
                        throw new Error(`load_local: only '.py' file extension; current not supported: ${extension} `);
                    }
                    console.log(`start loading file: '${filePath}'`);
                    const obj = await objects.get(repo, ref, filePath, presign);
                    console.log("fetched local script, running: ", obj)
                    const result = await pyodide.runPythonAsync(obj)
                    console.log("finished running sript ", result)
                    // const doc = (<TextDownloader
                    //     repoId={repo}
                    //     refId={ref}
                    //     path={filePath}
                    //     presign={presign}
                    //     onReady={text =>
                    //         console.log("DOWNLOADED LOCAL SCRIPT : ", text)
                    //     }
                    // />)

                }
            case "clear":
                clearOutputs();
                break;
            default:
                console.log(`Invalid magic command: ${funcName} `);
        }
    }
    const execCellCode = async (editorContent) => {
        clearOutputs();
        if (isPyodideLoaded && pyodide) {
            try {
                setLoading(true);
                const { magicCommands, codeContent } = parseEditorContent(editorContent);
                for (const cmd of magicCommands) {
                    if (cmd) {
                        console.log('Function Name:', cmd.funcName);
                        console.log('Arguments:', cmd.args);
                        // TODO(isan) configure presign from general conf
                        const presign = false;
                        try {
                            await handleMagic(cmd, repoId, refId, presign);
                        } catch (err) {
                            throw new Error(`Error executing magic '${cmd.funcName}': ${err} `);
                        }
                        console.log('Magic command executed.');
                    } else {
                        console.log('Invalid magic command line.');
                    }
                }
                const result = await pyodide.runPythonAsync(codeContent);
                let strResultOutput = !result ? "No output returned." : result.toString();
                setOutput(strResultOutput);
                setError(null);
                if (result) {
                    outputRef.current.push(result.toString());
                }
            } catch (err) {
                setError("Pyodide Error: " + err.toString());
            } finally {
                setLoading(false);
            }
        } else {
            setError("Pyodide is not loaded yet.");
        }
    }
    const handleSubmit = useCallback((event) => {
        event.preventDefault()
        execCellCode(code);
    }, [isPyodideLoaded, pyodide, code, output, error]);

    const renderOutputRef = () => {
        return outputRef.current.map((msg, idx) => { return <div style={{ whiteSpace: 'pre-wrap', fontFamily: 'monospace' }} key={idx}>{msg}</div> })
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
            return null;
        }
    }
    const renderPyodideLoadingIndicator = () => {
        return (
            <div>
                <Loading message="Loading Pyodide, (~10 seconds)..." />
            </div>
        )
    }
    return (
        <div>
            <Form ref={formRef} onSubmit={handleSubmit} onReset={clearAll}>
                <Form.Group className="mt-2 mb-1" controlId="objectQuery">
                    {!isPyodideLoaded ? renderPyodideLoadingIndicator() : null}
                    <div>
                        <div style={cellStyle}>
                            <EditorToolbar
                                defaultSnippetLabel={snippetStatObjectLabel}
                                loading={loading || !isPyodideLoaded}
                                loadingMode={!isPyodideLoaded ? "Loading pyodide" : "Executing"}
                                onSnippet={(snippet) => {
                                    clearAll();
                                    if (snippet.label === snippetReadObjectLabel) {
                                        setCode(initialCodeReadObjectTpl(repoId, refId, path))
                                    } else if (snippet.label === snippetMicropipListLabel) {
                                        setCode(microPiplistSnippet)
                                    } else if (snippet.label === snippetNumpyMatplotlibLabel) {
                                        setCode(numpyMatplotlibSnippet);
                                    } else if (snippet.label === snippetReadCurrentParquetLabel) {
                                        setCode(readCurrParquetTpl(repoId, refId, path, fileExtension));
                                    } else if (snippet.label === snippetReadCurrentImageLabel) {
                                        setCode(readCurrentImageSnippetTpl(repoId, refId, path, fileExtension));
                                    } else if (snippet.label === snippetMagicLoadLocal) {
                                        setCode(magicLoadLocalScriptSnippet());
                                    } else if (snippet.label === snippetStatObjectLabel) {
                                        setCode(statsCurrentObjectTpl(repoId, refId, path));
                                    }else
                                        setCode("TODO: " + snippet.label)
                                }
                                }
                                onRun={() => { }}
                                onClear={() => { }} />
                            <div style={editorContainerStyle}>
                                <AceEditor
                                    // commands={[
                                    //     {
                                    //         name: 'runCommand',
                                    //         bindKey: { win: 'Ctrl-Enter', mac: 'Ctrl-Enter' },
                                    //         exec: () => {
                                    //             // simulate form submission otherwise code state var is always empty
                                    //             if (formRef.current) {
                                    //                 formRef.current.dispatchEvent(
                                    //                     new Event('submit', { bubbles: true, cancelable: true })
                                    //                 );
                                    //             }

                                    //         },
                                    //     },
                                    // ]}
                                    placeholder={"import lakefs ..."}
                                    mode="python"
                                    theme="github"
                                    name="blah2"
                                    onLoad={(editor) => { console.log("ace editor loaded") }}
                                    onChange={(newCode) => { setCode(newCode) }}
                                    //defaultValue={}
                                    fontSize={14}
                                    width="100%"
                                    height="300px"
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
                <div id="image-container"></div>
                {renderOutputContent(output, error)}
            </div>
        </div>
    )
}