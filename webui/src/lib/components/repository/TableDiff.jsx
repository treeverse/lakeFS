import React, {useState} from "react";
import Table from "react-bootstrap/Table";
import {ChevronDownIcon, ChevronRightIcon} from "@primer/octicons-react";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import {Link} from "../nav";


export const TableDiff = () => {
    // TODO: use otfDiffAPiEndpoint to get otfdiffList and populate results
    const mockRes = '{"results": [{"version": "1", "timestamp": 1515491537026, "operation": "INSERT", "operationContent": {"operationParameters": {"mode": "Append","partitionBy": "[]"}}}, {"version": "2", "timestamp": 1515491537026, "operation": "DELETE", "operationContent": {"operationParameters": {"mode": "Append","partitionBy": "[]"}}}]}'

    let response = JSON.parse(mockRes)
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
    const [rowExpanded, setRowExpanded] = useState(false)
    return (<tr>
            <td className="col-10 table-operation-type">{otfDiff.operation}</td>
            <td className="col-1 table-version">Version = {otfDiff.version}</td>
            <td className="col-1 operation-expansion">
                <OperationExpansionSection operationExpanded={rowExpanded} onClick={() => setRowExpanded(!rowExpanded)}/>
            </td>
        </tr>
    )
}

// const TableOperationContent = ({content, onExpand}) => {
//     return <span>
//         operation content
//     </span>
// }

const OperationExpansionSection = ({operationExpanded, onClick}) => {
    return <OverlayTrigger placement="bottom" overlay={<Tooltip>{operationExpanded ? "Hide operation info" : "Show operation info"}</Tooltip>}>
                <span onClick={onClick}>{operationExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>}</span>
           </OverlayTrigger>
}

