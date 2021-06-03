import React, {useState} from "react";

import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import {CircleSlashIcon, FileDirectoryIcon, HistoryIcon, PencilIcon, PlusIcon, TrashIcon} from "@primer/octicons-react";

import {ConfirmationModal} from "../modals";
import {Link} from "../nav";


const ChangeRowActions = ({ entry, onRevert }) => {
    const [show, setShow] = useState(false);
    const revertConfirmMsg = `Are you sure you wish to revert "${entry.path}" (${entry.type})?`;
    const onSubmit = () => {
        onRevert(entry)
        setShow(false)
    };

    return (
        <>
            <OverlayTrigger key={"bottom"} overlay={(<Tooltip id={"revert-entry"}>Revert change</Tooltip>)}>
                <Button variant="link" disabled={false} onClick={(e) => {
                    e.preventDefault();
                    setShow(true)
                }} >
                    <HistoryIcon/>
                </Button>
            </OverlayTrigger>

            <ConfirmationModal show={show} onHide={() => setShow(false)} msg={revertConfirmMsg} onConfirm={onSubmit}/>
        </>
    );
};

export const ChangeEntryRow = ({ entry, showActions, relativeTo="", onNavigate, onRevert }) => {
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

    let pathText = entry.path;
    if (pathText.startsWith(relativeTo)) {
        pathText = pathText.substr(relativeTo.length, pathText.length);
    }

    let diffIndicator;
    switch (entry.type) {
        case 'removed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-removed"}>Removed</Tooltip>)}>
                    <span>
                        <TrashIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'added':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-added"}>Added</Tooltip>)}>
                    <span>
                        <PlusIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'changed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-changed"}>Changed</Tooltip>)}>
                    <span>
                        <PencilIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'conflict':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-conflict"}>Conflict</Tooltip>)}>
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
    if (showActions) {
        entryActions = <ChangeRowActions
            entry={entry}
            onRevert={onRevert}
        />;
    }

    return (
        <>
            <tr className={rowClass}>
                <td className="diff-indicator">
                    {diffIndicator}
                </td>
                <td className="tree-path">
                    {(entry.path_type === "common_prefix") ? (
                        <span>
                            <Link href={onNavigate(entry)}>
                                <FileDirectoryIcon/>
                                {pathText}
                            </Link>
                        </span>
                    ) : (
                        <span>{pathText}</span>
                    )
                    }
                </td>
                <td className={"tree-row-actions"}>
                    {entryActions}
                </td>
            </tr>
        </>
    );
};
