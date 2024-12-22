import React, { createContext, useEffect, useRef, useState } from 'react';

export const PyodideContext = createContext(null);

export const WithPyodideContext = ({ children }) => {
    const [isPyodideLoaded, setIsPyodideLoaded] = useState(false);
    const pyodideRef = useRef(null);

    useEffect(() => {
        const loadPyodideAndPackages = async () => {
            pyodideRef.current = await window.loadPyodide({
                indexURL: 'https://cdn.jsdelivr.net/pyodide/v0.26.4/full/',
            });
            // consider Sequential Package Loading maybe crashing memory?
            //   await pyodideRef.current.loadPackage([
            //     'pandas',
            //     'numpy',
            //     'matplotlib',
            //     'scikit-learn',
            //     'cloudpickle',
            //   ]);
            const packages = ['pandas', 'numpy', 'matplotlib', 'scikit-learn', 'cloudpickle'];
            for (const pkg of packages) {
                await pyodideRef.current.loadPackage(pkg);
            }
            // patches http tests 
            await pyodideRef.current.loadPackage("urllib3");
            await pyodideRef.current.loadPackage("requests");
            //  override stdout hook https://pyodide.org/en/stable/usage/api/js-api.html#pyodide.setStdout
            // pyodideRef.current.setStdout({ batched: (msg) => console.log(">>>>>>>>> [batched] " + msg) });
            console.log("1 init: loading micropip");
            await pyodideRef.current.loadPackage('micropip');
            console.log("2 init: using micropip");
            const micropip = pyodideRef.current.pyimport("micropip");
            console.log("3 init: installing lakefs");
            await micropip.install("ssl")
            await pyodideRef.current.runPythonAsync(`
                import micropip 
                await micropip.install("lakefs", deps=False)
                await micropip.install("lakefs-sdk", deps=False)
                await micropip.install("pydantic", deps=False)
                await micropip.install("python-dateutil")
                await micropip.install("PyYAML")
            `);
            // await micropip.install("lakefs");
            /////////////// some patch for requests to work
            // await micropip.install("pyodide-http")
            // const pyodideHttp = pyodideRef.current.pyimport("pyodide_http");
            // pyodideHttp.patch_all()
            // await micropip.install("urllib3>=2.2.1")
            // await micropip.install("requests")
            setIsPyodideLoaded(true);
        };

        loadPyodideAndPackages();
    }, []);

    return (
        <PyodideContext.Provider value={{ pyodide: pyodideRef.current, isPyodideLoaded }}>
            {isPyodideLoaded ? (
                children
            ) : (
                <div>Loading Pyodide and packages...</div>
            )}
        </PyodideContext.Provider>
    );
};