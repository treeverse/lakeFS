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

export const TreeItem = ({ entry, repo, reference, internalRefresh, onRevert, delimiter, after, relativeTo, getMore, depth=0 }) => {
    const [expanded, setExpanded] = useState(false);
    const [afterUpdated, setAfterUpdated] = useState(after);
    const [realRes, setRealRes] = useState([]);
    const [realPagin, setRealPagin] = useState({});

    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        if (!expanded) return
        if (!repo) return

        if (realRes.length > 0 && realRes.at(-1).path > afterUpdated) {
            // results already cached
            return realRes, realPagin
        }

        let { results, pagination } = await getMore(afterUpdated, entry.path)
        setRealRes(realRes.concat(results))
        setRealPagin(pagination)
        return {results, pagination}
    }, [repo.id, reference.id, internalRefresh, afterUpdated, entry.path, delimiter, expanded])


    if (!!error)
        return <Error error={error}/>
    if (realRes.length === 0 && loading)
        return <TreeEntryRow key={entry.path+"entry-row"} entry={entry} showActions={true} loading={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} />

    if (!entry.path.endsWith(delimiter)){
        return <TreeEntryRow key={entry.path+"entry-row"} entry={entry} showActions={true} leaf={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} />

    }
    return <>
            <TreeEntryRow key={entry.path+"entry-row"} entry={entry} showActions={true} expanded={expanded} relativeTo={relativeTo} depth={depth} onClick={() => setExpanded(!expanded)} onRevert={onRevert} />
            {expanded && realRes &&
                realRes.map(child =>
                    ( <TreeItem key={child.path+"-item"} entry={child} repo={repo} reference={reference} onRevert={onRevert}
                                internalReferesh={internalRefresh} delimiter={delimiter} depth={depth+1} after={after} relativeTo={entry.path} getMore={getMore}/>))}
            {!!nextPage &&
                <tr key={entry.path+"-row"}
                    className={"tree-entry-row diff-more"}
                    onClick={(_ => {
                        setAfterUpdated(nextPage)
                    })}
                >
                    <td className="diff-indicator"/>
                    <td className="tree-path">
                        <span style={{marginLeft:(depth+1)*10,color:"#007bff"}}>
                            {`Load more results for prefix ${entry.path} ....`}
                        </span>
                    </td>
                    <td/>
                </tr>
            }
          </>
}

export const TreeEntryRow = ({ entry, showActions, relativeTo="", leaf=false, expanded, depth=0, onClick, loading=false, onRevert }) => {
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathText = extractPathText(entry, relativeTo);
    let diffIndicator = diffIndicatorIcon(entry);
    let actions = entryActions(showActions, entry, onRevert);

    return (
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
    );
};

export const ChangeEntryRow = ({ entry, showActions, relativeTo="", onNavigate, onRevert }) => {
    let rowClass = 'tree-row ' + diffType(entry);
    let pathText = extractPathText(entry, relativeTo);
    let diffIndicator = diffIndicatorIcon(entry);
    let actions = entryActions(showActions, entry, onRevert);

    return (
        <tr className={rowClass}>
            <td className="diff-indicator">
                {diffIndicator}
            </td>
            <td className="tree-path">
                {(entry.path_type === "common_prefix") ? (
                    <span key={`link-${entry.path}`}>
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
