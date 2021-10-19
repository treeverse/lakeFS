import React, {useState} from "react";

import {OverlayTrigger, Tab, Tabs} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import {
    ArrowRightIcon, ChevronDownIcon, ChevronRightIcon,
    CircleSlashIcon, ClockIcon, CodeSquareIcon,
    FileDirectoryIcon,
    HistoryIcon,
    PencilIcon,
    PlusIcon,
    SidebarExpandIcon,
    TrashIcon
} from "@primer/octicons-react";

import {ConfirmationModal} from "../modals";
import {Link} from "../nav";
import {useAPIWithPagination} from "../../hooks/api";
import {branches, DEFAULT_LISTING_AMOUNT, refs} from "../../api";
import {Paginator} from "../pagination";
import {Error, Loading} from "../controls";


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

export const TreeItem = ({ entry, repo, reference, internalRefresh, onRevert, delimiter, after, relativeTo, depth=0 }) => {
    const [expanded, setExpanded] = useState(false);
    const [aa, setAa] = useState(after);

    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        console.log("wow12")
        if (!expanded) return
        if (!repo) return

        return refs.changes(repo.id, reference.id, aa, entry.path, delimiter)
    }, [repo.id, reference.id, internalRefresh, aa, entry.path, delimiter, expanded])

    if (!!error) return <Error error={error}/>
    if (loading) return <TreeEntryRow key={entry.path} entry={entry} showActions={true} loading={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} />

    if (!entry.path.endsWith(delimiter)){
        return  <>
                    <TreeEntryRow key={entry.path} entry={entry} showActions={true} leaf={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} />
                </>
    }
    return <>
            <TreeEntryRow key={entry.path} entry={entry} showActions={true} expanded={expanded} relativeTo={relativeTo} depth={depth} onClick={() => setExpanded(!expanded)} onRevert={onRevert} />
            {expanded && results ?
                results.map(child =>
                    ( <TreeItem key={entry.path+"-item"} entry={child} repo={repo} reference={reference} onRevert={onRevert}
                                internalReferesh={internalRefresh} delimiter={delimiter} depth={depth+1} after={after} relativeTo={entry.path}/>)) : ""}
            {!!nextPage && <tr><Button onClick={(event => {
                console.log("wow", after, aa, nextPage)
                setAa(nextPage)
            })}>{("for more results")}</Button></tr>}
          </>
}

export const TreeEntryRow = ({ entry, showActions, relativeTo="", leaf=false, expanded, depth=0, onClick, loading=false, onRevert }) => {
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathText = extractPathText(entry, relativeTo);
    let diffIndicator = diffIndicatorIcon(entry);
    let actions = entryActions(showActions, entry, onRevert);

    return (
        <>
            <tr className={rowClass} >
                <td className="diff-indicator">{diffIndicator}</td>
                <td onClick={onClick} className="tree-path">
                    <span style={{marginLeft:depth*10}}>
                        {leaf ? "" : expanded ? <ChevronDownIcon/>:<ChevronRightIcon/>}
                        {loading ? <ClockIcon/> : ""}
                        {pathText}
                    </span>
                </td>
                <td className={"tree-row-actions"}>{actions}</td>
            </tr>
        </>
    );
};

export const ChangeEntryRow = ({ entry, showActions, relativeTo="", onNavigate, onRevert }) => {
    let rowClass = 'tree-row ' + diffType(entry);
    let pathText = extractPathText(entry, relativeTo);
    let diffIndicator = diffIndicatorIcon(entry);
    let actions = entryActions(showActions, entry, onRevert);

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
                                {pathText}
                            </Link>
                        </span>
                    ) : (
                        <span>{pathText}</span>
                    )}
                </td>
                <td className={"tree-row-actions"}>
                    {actions}
                </td>
            </tr>
        </>
    );
};

function extractPathText(entry, relativeTo) {
    let pathText = entry.path;
    if (pathText.startsWith(relativeTo)) {
        pathText = pathText.substr(relativeTo.length, pathText.length);
    }
    return pathText;
}

function diffType(entry) {
    switch (entry.type) {
        case 'changed':
            return 'diff-changed';
        case 'added':
            return 'diff-added';
        case 'removed':
            return 'diff-removed';
        case 'conflict':
            return 'diff-conflict';
        default:
            return '';
    }
}

function diffIndicatorIcon(entry) {
    switch (entry.type) {
        case 'removed':
            return <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-removed"}>Removed</Tooltip>)}>
                        <span>
                            <TrashIcon/>
                        </span>
                    </OverlayTrigger>;
        case 'added':
            return <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-added"}>Added</Tooltip>)}>
                        <span>
                            <PlusIcon/>
                        </span>
                    </OverlayTrigger>;
        case 'changed':
            return <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-changed"}>Changed</Tooltip>)}>
                        <span>
                            <PencilIcon/>
                        </span>
                    </OverlayTrigger>;
        case 'conflict':
            return <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-conflict"}>Conflict</Tooltip>)}>
                        <span>
                            <CircleSlashIcon/>
                        </span>
                    </OverlayTrigger>;
        default:
            return '';
    }
}

function entryActions(showActions, entry, onRevert) {
    let actions;
    if (showActions) {
        actions = <ChangeRowActions
            entry={entry}
            onRevert={onRevert}
        />;
    }
    return actions;
}
