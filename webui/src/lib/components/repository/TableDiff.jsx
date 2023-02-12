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
    let response = useAPI(() => repositories.otfDiff(repo.id, leftRef, rightRef, tablePath, OtfType.Delta), [])
    if (response && response.loading) return <Loading style={{margin: 0+"px"}}/>;
    const err = response && response.error;
    if (err) return <Error error={"Table not found"}/>;

    const otfDiffs = response.response.results;
    return <>
        {(otfDiffs.length === 0) ?  <Alert variant="info" style={{margin: 0+"px"}}>No changes</Alert> :
            <Table classname="table-diff" borderless size="md" style={{"table-layout": "fixed"}}>
                <tbody>
                {
                    response.response.results.map(otfDiff => {
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
        <td className="pl-lg-10 col-10 table-operation-type" style={{"padding-left": 30+"px"}}>{otfDiff.operation}</td>
        <td className="col-sm-auto table-version" style={{"text-align": "left", "width": "8%"}}>Version = {otfDiff.version}</td>
        <td className="col-sm-auto operation-expansion" style={{"padding-right": 0 + "px", "width": "4%"}}>
            <OperationExpansionSection operationExpanded={operationExpanded} onExpand={onExpand}/>
        </td>
    </tr>

}

const OperationDetailsRow = ({otfDiff}) => {
    const operationTimestamp = otfDiff.timestamp;
    const operationContent = parseOperationContent(otfDiff.operation_content);
    return <tr className="otf-diff-operation-details">
        <td className="pl-lg-10 col-10 operation-details" style={{"padding-left": 30+"px"}}>
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
        <Button variant="link" style={{color: 'black'}} onClick={onExpand}>{operationExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>}</Button>
    </OverlayTrigger>
}

function operationToDiffType(operation) {
    const diffType = deltaLakeOperationToDiffType.get(operation);
    return diffType !== undefined ? diffType : DiffType.Changed;
}

function parseOperationContent(content) {
    let parsedContent = "";
    const JSONContent = JSON.parse(JSON.stringify(content, null, 2));
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
