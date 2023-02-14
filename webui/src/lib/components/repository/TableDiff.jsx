import React, {useState} from "react";
import Table from "react-bootstrap/Table";
import {ChevronDownIcon, ChevronRightIcon} from "@primer/octicons-react";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import {DiffType, OtfType} from "../../../constants";
import {useAPI} from "../../hooks/api";
import {repositories} from "../../api";
import {Error, Loading} from "../controls";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";

// The list of available operations is based on: https://docs.databricks.com/delta/history.html#operation-metrics-keys
const deltaLakeOperationToDiffType = new Map([
    ["WRITE" , DiffType.Added],
    ["INSERT" , DiffType.Added], // We see this returning from describe history operation although not listed.
    ["DELETE" , DiffType.Removed],
    ["CREATE TABLE AS SELECT" , DiffType.Added],
    ["REPLACE TABLE AS SELECT" , DiffType.Changed],
    ["COPY INTO" , DiffType.Changed],
    ["STREAMING UPDATE", DiffType.Changed],
    ["TRUNCATE" , DiffType.Removed] ,
    ["MERGE" , DiffType.Changed] ,
    ["UPDATE" , DiffType.Changed] ,
    ["FSCK" , DiffType.Removed] ,
    ["CONVERT" , DiffType.Added] ,
    ["OPTIMIZE" , DiffType.Changed],
    ["RESTORE" , DiffType.Changed] ,
    ["VACUUM" , DiffType.Removed],
]);

export const TableDiff = ({repo, leftRef, rightRef, tablePath}) => {
    let { error, loading, response } = useAPI(() => repositories.otfDiff(repo.id, leftRef, rightRef, tablePath, OtfType.Delta), [])
    if (loading) return <Loading style={{margin: 0+"px"}}/>;
    if (!loading && error) return <Error error={error}/>;

    const otfDiffs = response.results;
    return <>
        {(otfDiffs.length === 0) ?  <Alert variant="info" style={{margin: 0+"px"}}>No changes</Alert> :
            <Table className="table-diff" size="md">
                <tbody>
                {
                    response.results.map(otfDiff => {
                        return <OtfDiffRow key={otfDiff.timestamp + "-diff-row"} otfDiff={otfDiff}/>;
                    })
                }
                </tbody>
            </Table>
        }
    </>
}

const OtfDiffRow = ({otfDiff}) => {
    const [rowExpanded, setRowExpanded] = useState(false);
    const rowClass = "otf-diff-" + operationToDiffType(otfDiff.operation);
    return <>
        <OperationMetadataRow className={rowClass} otfDiff={otfDiff} operationExpanded={rowExpanded} onExpand={() => setRowExpanded(!rowExpanded)}/>
        {rowExpanded
            ? <OperationDetailsRow otfDiff={otfDiff}/>
            : ""}
    </>
}

const OperationMetadataRow = ({otfDiff, operationExpanded, onExpand, ...rest}) => {
    return <tr {...rest}>
        <td className={"table-operation-type pl-lg-10 col-10"}>{otfDiff.operation}</td>
        <td className="table-version col-sm-auto">Version = {otfDiff.version}</td>
        <td className="operation-expansion col-sm-auto">
            <OperationExpansionSection operationExpanded={operationExpanded} onExpand={onExpand}/>
        </td>
    </tr>

}

const OperationDetailsRow = ({otfDiff}) => {
    const operationTimestamp = otfDiff.timestamp;
    const operationContent = parseOperationContent(otfDiff.operation_content);
    return <tr className="otf-diff-operation-details">
        <td className="pl-lg-10 col-10 table-operation-details">
            <strong>Timestamp:</strong> {operationTimestamp}
            <br/>
            <strong>Commit Info:</strong>
            <br/>
            <pre>{operationContent}</pre>
        </td>
    </tr>
}

const OperationExpansionSection = ({operationExpanded, onExpand}) => {
    return <OverlayTrigger placement="bottom" overlay={<Tooltip>{operationExpanded ? "Hide operation info" : "Show operation info"}</Tooltip>}>
        <Button variant="link" className="table-operation-expansion" onClick={onExpand}>{operationExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>}</Button>
    </OverlayTrigger>
}

function operationToDiffType(operation) {
    const diffType = deltaLakeOperationToDiffType.get(operation);
    return diffType !== undefined ? diffType : DiffType.Changed;
}

/**
 * Parses otf operation content. operations sometimes include unparsed json strings and this methods aims to simplify
 * reading those operations.
 *
 * @param content of the table operation
 * @return a prettified string including the operation content.
 */
function parseOperationContent(content) {
    let parsedContent = "";
    const JSONContentStr = JSON.stringify(content, null, 2);
    const JSONContent = JSON.parse(JSONContentStr);
    for (let key in JSONContent) {
        const val = JSONContent[key];
        parsedContent += `${key}: `
        let parsedVal = "";
        try {
            parsedVal = JSON.parse(val);
        } catch (err) {
            parsedVal = val;
        }
        parsedContent += parsedVal + "\n";
    }
    return parsedContent;
}
