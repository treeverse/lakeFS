import React, {useRef, useState} from "react";
import {
    ExecuteImportButton,
    ImportDone,
    ImportForm,
    ImportPhase,
    ImportProgress,
    runImport
} from "../services/import_data";
import {Error, Loading} from "../../../lib/components/controls";
import {useAPI} from "../../../lib/hooks/api";
import {config} from "../../../lib/api";

const ImportDataStep = ({repoId, branchName, onComplete, prependPath = ''}) => {
    const [importPhase, setImportPhase] = useState(ImportPhase.NotStarted);
    const [numberOfImportedObjects, setNumberOfImportedObjects] = useState(0);
    const [isImportEnabled, setIsImportEnabled] = useState(false);
    const [importError, setImportError] = useState(null);
    const sourceRef = useRef(null);
    const commitMsgRef = useRef(null);
    const {response, error, loading} = useAPI(() => config.getStorageConfig());

    if (loading) {
        return <Loading/>
    }

    const showError = importError ? importError : error;

    const doImport = async () => {
        setImportPhase(ImportPhase.InProgress);
        const updateStateFromImport = ({importPhase, numObj}) => {
            setImportPhase(importPhase);
            setNumberOfImportedObjects(numObj)
        }
        try {
            const source = sourceRef.current.value;
            await runImport(
                updateStateFromImport,
                prependPath,
                commitMsgRef.current.value,
                source,
                branchName,
                repoId
            );
            onComplete({importLocation: source});
        } catch (error) {
            setImportError(error);
            setImportPhase(ImportPhase.Failed);
            setIsImportEnabled(false);
        }
    }

    return (
        <>
            <h3 className={"wizard-step-header"}>Import Data</h3>
            {
                (importPhase === ImportPhase.NotStarted || importPhase === ImportPhase.Failed) &&
                <ImportForm
                    config={response}
                    pathStyle={{'minWidth': '25%'}}
                    sourceRef={sourceRef}
                    updateSrcValidity={(isValid) => setIsImportEnabled(isValid)}
                    repoId={repoId}
                    importBranch={branchName}
                    commitMsgRef={commitMsgRef}
                    shouldAddPath={false}/>
            }
            {
                importPhase === ImportPhase.InProgress &&
                <ImportProgress numObjects={numberOfImportedObjects}/>
            }
            {
                importPhase === ImportPhase.Completed &&
                <ImportDone currBranch={branchName} importBranch={branchName} numObjects={numberOfImportedObjects}/>
            }
            {
                showError && <Error error={showError}/>
            }
            {
                <ExecuteImportButton importPhase={importPhase} importFunc={doImport} isEnabled={isImportEnabled}/>
            }

        </>
    );
}

export default ImportDataStep
