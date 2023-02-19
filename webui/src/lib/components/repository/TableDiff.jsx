import React, {useState} from "react";
import Table from "react-bootstrap/Table";
import {ChevronDownIcon, ChevronRightIcon, InfoIcon} from "@primer/octicons-react";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import {OtfDiffType, OtfType} from "../../../constants";
import {useAPI} from "../../hooks/api";
import {repositories} from "../../api";
import {Error, Loading} from "../controls";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";

export const DeltaLakeDiff = ({repo, leftRef, rightRef, tablePath}) => {
    let { error, loading, response } = useAPI(() => repositories.otfDiff(repo.id, leftRef, rightRef, tablePath, OtfType.Delta), [])
    if (loading) return <Loading style={{margin: 0+"px"}}/>;
    if (!loading && error) return <Error error={error}/>;

    const otfDiffs = response.results;
    const diffType = response.diff_type;
    return <>
        {(OtfDiffType.Dropped != diffType && otfDiffs.length === 0) ?  <Alert variant="info" style={{margin: 0+"px"}}>No changes</Alert> :
            <Table className="table-diff" size="md">
                <tbody>
                <TableDiffTypeRow diffType={diffType}/>
                {
                    otfDiffs.map(otfDiff => {
                        return <OtfDiffRow key={otfDiff.timestamp + "-diff-row"} otfDiff={otfDiff}/>;
                    })
                }
                </tbody>
            </Table>
        }
    </>
}

const TableDiffTypeRow = ({diffType}) => {
    if (OtfDiffType.Changed == diffType) {
        return "";
    }
    return <tr>
        <td className="table-diff-type pl-lg-10 col-10"><InfoIcon/> Table {diffType}</td>
        <td className="table-version-placeholder col-sm-auto"></td>
        <td className="operation-expansion-placeholder col-sm-auto"></td>
    </tr>
}

const OtfDiffRow = ({otfDiff}) => {
    const [rowExpanded, setRowExpanded] = useState(false);
    const rowClass = "otf-diff-" + otfDiff.operation_type;
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
    const operationContent = stringifyOperationContent(otfDiff.operation_content);
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

/**
 * Stringifies otf operation content. operations sometimes include unparsed json strings and this methods aims to simplify
 * reading those operations.
 *
 * @param content of the table operation
 * @return a prettified string including the operation content.
 */
function stringifyOperationContent(content) {
    let jsonData = {};
    for (const [key, value] of Object.entries(content)) {
        jsonData[key] = parseValue(value);
    }
    return JSON.stringify(jsonData, null, 4);
}

function parseValue(val) {
    let parsedVal = "";
    try {
        parsedVal = JSON.parse(val);
    } catch (err) {
        parsedVal = val;
    }
    return parsedVal;
}
