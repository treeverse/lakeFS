import React, {useRef, useState} from "react";
import {
    ExecuteImportButton,
    ImportDone,
    ImportForm,
    ImportPhase,
    ImportProgress,
    runImport
} from "../services/import_data";
import {Error} from "../../../lib/components/controls";

const ImportDataStep = ({repoId, branchName, onComplete, prependPath = ''}) => {
    const [importPhase, setImportPhase] = useState(ImportPhase.NotStarted);
    const [numberOfImportedObjects, setNumberOfImportedObjects] = useState(0);
    const [isSourceValid, setIsSourceValid] = useState(false);
    const [importError, setImportError] = useState(null);
    const sourceRef = useRef(null);
    const commitMsgRef = useRef(null);

    const doImport = async () => {
        setImportPhase(ImportPhase.InProgress);
        const updateStateFromImport = ({importPhase, numObj}) => {
            setImportPhase(importPhase);
            setNumberOfImportedObjects(numObj)
        }
        try {
            await runImport(
                updateStateFromImport,
                prependPath,
                commitMsgRef.current.value,
                sourceRef.current.value,
                branchName,
                repoId
            );
            onComplete();
        } catch (error) {
            setImportError(error)
            throw error
        }
    }

    return (
        <>
            <h2 className={"wizard-step-header"}>Import Data</h2>
            {
                importPhase === ImportPhase.NotStarted &&
                <ImportForm
                    pathStyle={{'minWidth': '25%'}}
                    sourceRef={sourceRef}
                    updateSrcValidity={(isValid) => setIsSourceValid(isValid)}
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
                importError && <Error error={importError}/>
            }
            <ExecuteImportButton importPhase={importPhase} importFunc={doImport} isSourceValid={isSourceValid}/>
        </>
    );
}

export default ImportDataStep
