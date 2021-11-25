import React, {useState} from "react";

import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import {
    ChevronDownIcon,
    ChevronRightIcon,
    CircleSlashIcon,
    ClockIcon,
    FileDirectoryIcon,
    HistoryIcon,
    PencilIcon,
    PlusIcon,
    TrashIcon,
    FileDiffIcon
} from "@primer/octicons-react";

import {ConfirmationModal} from "../modals";
import {Link} from "../nav";
import {useAPIWithPagination} from "../../hooks/api";
import {Error} from "../controls";
import {ObjectsDiff} from "./ObjectsDiff";


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

/*
    Tree item is a node in the tree view. It can be expanded to multiple TreeEntryRow:
    1. A single TreeEntryRow for the current prefix (or entry for leaves).
    2. Multiple TreeItem as children, each representing another tree node.
    entry: The entry the TreeItem is representing, could be either an object or a prefix.
    repo: Repository
    reference: commitID / branch
    leftDiffRefID: commitID / branch
    rightDiffRefID: commitID / branch
    internalRefresh: to be called when the page refreshes manually
    onRevert: to be called when an object/prefix is requested to be reverted
    delimiter: objects delimiter ('' or '/')
    after: all entries must be greater than after
    relativeTo: prefix of the parent item ('' for root elements)
    getMore: callback to be called when more items need to be rendered
    depth: the item's depth withing the tree
 */
export const TreeItem = ({ entry, repo, reference, leftDiffRefID, rightDiffRefID, internalRefresh, onRevert, onNavigate, delimiter, relativeTo, getMore, depth=0 }) => {
    const [dirExpanded, setDirExpanded] = useState(false); // state of a non-leaf item expansion
    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({results:[], pagination:{}}); // current retrieved children of the item
    const [diffExpanded, setDiffExpanded] = useState(false); // state of a leaf item expansion

    const { error, loading, nextPage } = useAPIWithPagination(async () => {
        if (!dirExpanded) return
        if (!repo) return

        if (resultsState.results.length > 0 && resultsState.results.at(-1).path > afterUpdated) {
            // results already cached
            return {results:resultsState.results, pagination: resultsState.pagination}
        }

        const { results, pagination } =  await getMore(afterUpdated, entry.path)
        setResultsState({results: resultsState.results.concat(results), pagination: pagination})
        return {results:resultsState.results, pagination: pagination}
    }, [repo.id, reference.id, internalRefresh, afterUpdated, entry.path, delimiter, dirExpanded])

    const results = resultsState.results
    if (!!error)
        return <Error error={error}/>

    if (loading && results.length === 0)
        return <TreeEntryRow key={entry.path+"entry-row"} entry={entry} showActions={true} loading={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} onNavigate={onNavigate} repo={repo} reference={reference}/>

    // When the entry represents a tree leaf
    if (!entry.path.endsWith(delimiter))
        return <>
            <TreeEntryRow key={entry.path+"entry-row"} entry={entry} showActions={true} leaf={true} relativeTo={relativeTo} depth={depth === 0 ? 0 : depth + 1} onRevert={onRevert}  onNavigate={onNavigate} repo={repo}
                                     reference={reference} diffExpanded={diffExpanded} onClick={() => setDiffExpanded(!diffExpanded)}/>
                {diffExpanded && <ExpandedLeafRow entry={entry} repoId={repo.id} leftDiffRef={leftDiffRefID} rightDiffRef={rightDiffRefID} depth={depth} loading={loading}/>
                }
            </>

    return <>
            <TreeEntryRow key={entry.path+"entry-row"} entry={entry} showActions={true} dirExpanded={dirExpanded} relativeTo={relativeTo} depth={depth} onClick={() => setDirExpanded(!dirExpanded)} onRevert={onRevert} onNavigate={onNavigate}  repo={repo} reference={reference}/>
            {dirExpanded && results &&
                results.map(child =>
                    ( <TreeItem key={child.path+"-item"} entry={child} repo={repo} reference={reference} leftDiffRefID={leftDiffRefID} rightDiffRefID={rightDiffRefID} onRevert={onRevert} onNavigate={onNavigate}
                                internalReferesh={internalRefresh} delimiter={delimiter} depth={depth+1} relativeTo={entry.path} getMore={getMore}/>))}
            {(!!nextPage || loading) &&
                <TreeEntryPaginator path={entry.path} depth={depth} loading={loading} nextPage={nextPage} setAfterUpdated={setAfterUpdated}/>
            }
          </>
}

export const TreeEntryRow = ({ entry, showActions, relativeTo="", leaf=false, dirExpanded, depth=0, onClick, loading=false, onRevert, onNavigate}) => {
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathSection = extractPathText(entry, relativeTo);
    let diffIndicator = diffIndicatorIcon(entry);
    let actions = entryActions(showActions, entry, onRevert);

    if (entry.path_type === "common_prefix"){
        pathSection = <Link href={onNavigate(entry)}>{pathSection}</Link>
    }
    return (
        <tr className={rowClass} >
            <td className="diff-indicator">{diffIndicator}</td>
            <td className="tree-path">
                <span style={{marginLeft: depth * 20 + "px"}}>
                    <span onClick={onClick}>
                        {leaf
                            ? entry.type === 'conflict'
                                ? ""
                                : <OverlayTrigger placement="bottom" overlay={<Tooltip>Show changes</Tooltip>}>
                                    <Link style={{verticalAlign: "revert"}} disabled={false} onClick={(e) => {
                                        e.preventDefault();
                                    }} >
                                    <FileDiffIcon/>
                                    </Link>
                                </OverlayTrigger>
                            : dirExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>}
                    </span>
                    {loading ? <ClockIcon/> : ""}
                    {pathSection}
                </span>
            </td>
            { actions === null ?
                <td/> :
                <td className={"change-entry-row-actions"}>{actions}</td>
            }
        </tr>
    );
};

export const TreeEntryPaginator = ({ path, setAfterUpdated, nextPage, depth=0, loading=false }) => {
    let pathSectionText = "Load more results ...";
    if (path !== ""){
        pathSectionText = `Load more results for prefix ${path} ....`
    }
    return (
        <tr key={"row-" + path}
            className={"tree-entry-row diff-more"}
            onClick={(_ => {
                setAfterUpdated(nextPage)
            })}
        >
            <td className="diff-indicator"/>
            <td className="tree-path">
                <span style={{marginLeft: depth * 20 + "px",color:"#007bff"}}>
                    {loading && <ClockIcon/>}
                    {pathSectionText}
                </span>
            </td>
            <td/>
        </tr>
    );
};

const ExpandedLeafRow = ({entry, repoId, leftDiffRef, rightDiffRef, loading}) => {
    return (
        <tr key={"row-" + entry.path} className={"leaf-entry-row"}>
            <td className="objects-diff" colSpan={3}>
                <ObjectsDiff
                    diffType={entry.type}
                    repoId={repoId}
                    leftRef={leftDiffRef}
                    rightRef={rightDiffRef}
                    path={entry.path}
                />
                {loading && <ClockIcon/>}
            </td>
        </tr>
    );
}

function extractPathText(entry, relativeTo) {
    let pathText = entry.path;
    if (pathText.startsWith(relativeTo)) {
        pathText = pathText.substr(relativeTo.length);
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
    if (entry.path_type === 'common_prefix') {
        return <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-prefix"}>Prefix</Tooltip>)}>
                        <span>
                            <FileDirectoryIcon fill={"#d9b63e"}/>
                        </span>
               </OverlayTrigger>;
    }

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
    if (!!!onRevert){
        return null
    }
    let actions;
    if (showActions) {
        actions = <ChangeRowActions
            entry={entry}
            onRevert={onRevert}
        />;
    }
    return actions;
}
