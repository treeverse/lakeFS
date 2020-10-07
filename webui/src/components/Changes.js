import React, {useState} from "react";
import Alert from "react-bootstrap/Alert";
import Table from "react-bootstrap/Table";
import {
    HistoryIcon,
    PencilIcon,
    PlusIcon,
    CircleSlashIcon,
    TrashcanIcon
} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import {connect} from "react-redux";
import {listBranches} from "../actions/branches";
import Card from "react-bootstrap/Card";
import {resetRevertBranch, revertBranch} from "../actions/branches";
import ConfirmationModal from "./ConfirmationModal";

const ChangeRowActions = connect(
    ({ branches }) => ({ revert: branches.revert }),
    ({ revertBranch, resetRevertBranch })
)(({repo, refId, entry, revertBranch, revert}) => {
    const [show, setShow] = useState(false);  
    const handleClose = () => setShow(false);
    const handleShow = () => setShow(true);
    const revertConfirmMsg = `are you sure you wish to revert "${entry.path}" (${entry.type})?`
    const onSubmit = () => revertBranch(repo.id, refId.id, {type: "object", path: entry.path});

    return (
        <>
            <OverlayTrigger key={"bottom"} overlay={(<Tooltip id={"revert-entry"}>revert change</Tooltip>)}>
                <Button variant="link" disabled={revert.inProgress} onClick={(e) => {
                        e.preventDefault();
                        if (revert.inProgress) {
                            return;
                        }
                        handleShow();
                    }} >
                    <HistoryIcon/>
                </Button>
            </OverlayTrigger>
        <ConfirmationModal show={show} onHide={handleClose} msg={revertConfirmMsg} onConfirm={onSubmit}/>
        </>
    );
});

const ChangeEntryRow = ({repo, refId, entry, showActions}) => {
    let rowClass = 'tree-row ';
    switch (entry.type) {
        case 'changed':
            rowClass += 'diff-changed';
            break;
        case 'added':
            rowClass += 'diff-added';
            break;
        case 'removed':
            rowClass += 'diff-removed';
            break;
        case 'conflict':
            rowClass += 'diff-conflict';
            break;
        default:
            break;
    }

    const pathText = entry.path;

    let diffIndicator;
    switch (entry.type) {
        case 'removed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-removed"}>removed</Tooltip>)}>
                    <span>
                        <TrashcanIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'added':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-added"}>added</Tooltip>)}>
                    <span>
                        <PlusIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'changed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-changed"}>changed</Tooltip>)}>
                    <span>
                        <PencilIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'conflict':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-conflict"}>conflict</Tooltip>)}>
                    <span>
                        <CircleSlashIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        default:
            break;
    }

    let entryActions;
    if (showActions && entry.path_type === 'object') {
        entryActions = <ChangeRowActions repo={repo} refId={refId} entry={entry}/>;
    }

    return (
        <>
            <tr className={rowClass}>
                <td className="diff-indicator">
                    {diffIndicator}
                </td>
                <td className="tree-path">
                    <span>{pathText}</span>
                </td>
                <td className={"tree-row-actions"}>
                    {entryActions}
                </td>
            </tr>
        </>
    );
};

const Changes = ({list, repo, refId, showActions}) => {
    const results = list.payload ? list.payload.results : [];
    let body;
    if (list.loading) {
        body = (<Alert variant="info">Loading...</Alert>);
    } else if (list.error) {
        body = <Alert variant="danger" className="tree-error">{list.error}</Alert>
    } else if (results.length === 0) {
        body = (<Alert variant="info">No changes</Alert>);
    } else {
        body = (
            <Table borderless size="sm">
                <tbody>
                {results.map(entry => (
                    <ChangeEntryRow
                        key={entry.path}
                        entry={entry}
                        repo={repo}
                        refId={refId}
                        showActions={showActions}/>
                ))}
                </tbody>
            </Table>
        );
    }
    return (
        <div className="tree-container">
            <Card>
                {body}
            </Card>
        </div>
    );
};

export default connect(
    ({branches}) => ({
        listBranchesState: branches.list,
    }),
    ({listBranches})
)(Changes);
