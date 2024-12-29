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
            const packages = ['pandas', 'numpy', 'matplotlib', 'scikit-learn', 'cloudpickle'];
            for (const pkg of packages) {
                await pyodideRef.current.loadPackage(pkg);
            }
            // patches http tests 
            await pyodideRef.current.loadPackage("urllib3");
            await pyodideRef.current.loadPackage("requests");
            //  override stdout hook https://pyodide.org/en/stable/usage/api/js-api.html#pyodide.setStdout
            // pyodideRef.current.setStdout({ batched: (msg) => console.log(">>>>>>>>> [batched] " + msg) });
            await pyodideRef.current.loadPackage('micropip');
            const micropip = pyodideRef.current.pyimport("micropip");
            await micropip.install("ssl")
            // required for parsing parquet in pandas 
            await micropip.install("fastparquet")
            // charting library
            await micropip.install("Matplotlib")
            // await micropip.install('ipython')
            await pyodideRef.current.runPythonAsync(`
                import micropip 
                await micropip.install("lakefs", deps=False)
                await micropip.install("lakefs-sdk", deps=False)
                await micropip.install("pydantic", deps=False)
                await micropip.install("python-dateutil")
                await micropip.install("PyYAML")
            `);
            setIsPyodideLoaded(true);
        };
        loadPyodideAndPackages();
    }, []);

    return (
        <PyodideContext.Provider value={{ pyodide: pyodideRef.current, isPyodideLoaded }}>
            {children}
        </PyodideContext.Provider>
        // <PyodideContext.Provider value={{ pyodide: pyodideRef.current, isPyodideLoaded }}>
        //     {isPyodideLoaded ? (
        //         children
        //     ) : (
        //         <div>Loading Pyodide and packages...</div>
        //     )}
        // </PyodideContext.Provider>
    );
};