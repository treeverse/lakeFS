import {Alert} from "react-bootstrap";
import {API_ENDPOINT} from "../actions/api";
import ClipboardButton from "./ClipboardButton";
import React from "react";


function extractActionRunID(err) {
    const m = /^Error: (\S+) hook aborted, run id '([^']+)'/.exec(err);
    return m ? m[2] : '';
}

function extractActionHookRunID(err) {
    const m = /^\t\* hook run id '([^']+)' failed/.exec(err);
    return m ? m[1] : '';
}

export function formatAlertText(repositoryId, err) {
    if (!err) {
        return '';
    }
    const lines = err.split('\n');
    if (lines.length === 1) {
        return <Alert.Heading>{err}</Alert.Heading>;
    }
    const runID = extractActionRunID(err);
    let result = lines.map((line, i) => {
        if (runID) {
            const hookRunID = extractActionHookRunID(line);
            if (hookRunID) {
                const link = `${API_ENDPOINT}/repositories/${repositoryId}/actions/runs/${runID}/hooks/${hookRunID}/output`;
                return <p key={i}><Alert.Link target="_blank" download={runID+'-'+hookRunID+'.log'} href={link}>{line}</Alert.Link></p>;
            }
        }
        return <p key={i}>{line}</p>;
    });
    if (runID) {
        const cmd = `lakectl actions runs describe lakefs://${repositoryId} ${runID}`;
        result = <>{result}<hr/>For detailed information run:<br/>{cmd}<ClipboardButton variant="link" text={cmd} tooltip="Copy"/></>;
    }
    return result;
}
