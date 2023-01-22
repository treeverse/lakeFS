import React, {useState} from "react";
import Table from "react-bootstrap/Table";
import {ChevronDownIcon, ChevronRightIcon} from "@primer/octicons-react";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";

export const TableDiff = () => {
    // TODO: use otfDiffAPiEndpoint to get otfdiffList and populate results
    const mockRes = '{"results": [{"version": "1", "timestamp": 1515491537026, "operation": "INSERT", "operationContent": {"operationParameters": {"mode": "Append","partitionBy": "[]"}}}, {"version": "2", "timestamp": 1515491537346, "operation": "DELETE", "operationContent": {"operationParameters": {"mode": "Append","partitionBy": "[]"}}}]}'

    let response = JSON.parse(mockRes);
    return <Table borderless size="sm">
            <tbody>
            {
                response.results.map(otfDiff => {
                    return <OtfDiffRow key={otfDiff.timeStamp + "-diff-row"} otfDiff={otfDiff}/>;
                })
            }
            </tbody>
        </Table>
}

const OtfDiffRow = ({otfDiff}) => {
    const [rowExpanded, setRowExpanded] = useState(false);
    return <>
            <OperationMetadataRow otfDiff={otfDiff} operationExpanded={rowExpanded} onExpand={() => setRowExpanded(!rowExpanded)}/>
            {rowExpanded
                ? <OperationDetailsRow otfDiff={otfDiff}/>
            : ""}
    </>
}

const OperationMetadataRow = ({otfDiff, operationExpanded, onExpand}) => {
    return <tr>
        <td className="col-10 table-operation-type">{otfDiff.operation}</td>
        <td className="col-1 table-version">Version = {otfDiff.version}</td>
        <td className="col-1 operation-expansion">
            <OperationExpansionSection operationExpanded={operationExpanded} onExpand={onExpand}/>
        </td>
    </tr>

}

const OperationDetailsRow = ({otfDiff}) => {
    const operationTimestamp = otfDiff.timestamp;
    const operationContent = JSON.stringify(otfDiff.operationContent);
    return <tr>
        <div>
            Timestamp: {operationTimestamp}
        </div>
        <div>
            Operation Content:
            <br/>
            {operationContent}
        </div>
    </tr>
}

const OperationExpansionSection = ({operationExpanded, onExpand}) => {
    return <OverlayTrigger placement="bottom" overlay={<Tooltip>{operationExpanded ? "Hide operation info" : "Show operation info"}</Tooltip>}>
                <span onClick={onExpand}>{operationExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>}</span>
           </OverlayTrigger>
}
