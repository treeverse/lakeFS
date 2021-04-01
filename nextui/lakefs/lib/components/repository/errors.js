import {Alert} from "react-bootstrap";
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
    const runID = extractActionRunID(err);
    if (lines.length === 1) {
        return <Alert.Heading>{err}</Alert.Heading>;
    }
    let result = lines.map((line, i) => {
        if (runID) {
            const hookRunID = extractActionHookRunID(line);
            let link =  `/repositories/${repositoryId}/actions/${runID}`
            if (hookRunID) {
                link = `/repositories/${repositoryId}/actions/${runID}/${hookRunID}`
            }
            return <p key={i}><Alert.Link href={link}>{line}</Alert.Link></p>;
        }
        return <p key={i}>{line}</p>;
    });

    return result;
}
