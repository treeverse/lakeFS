import React, {useRef, useState} from "react";

import {Overlay, OverlayTrigger, Popover} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import {
    ChevronDownIcon,
    ChevronRightIcon,
    CircleSlashIcon,
    ClockIcon,
    FileDiffIcon,
    FileDirectoryIcon,
    GraphIcon,
    HistoryIcon,
    PencilIcon,
    PlusIcon,
    TrashIcon
} from "@primer/octicons-react";

import {ConfirmationModal} from "../modals";
import {Link} from "../nav";
import {useAPIWithPagination} from "../../hooks/api";
import {Error} from "../controls";
import {ObjectsDiff} from "./ObjectsDiff";
import ChangeSummary from "./ChangeSummary";

/**
 * @param {(after : string, path : string, useDelimiter :? boolean) => Promise<any>} getMore - function to use to get the diff.
 */
const ChangeRowActions = ({entry, onRevert, diffExpanded, onClickExpandDiff, showSummary, onClickShowSummary, getMore}) => {
    const [showConfirmRevert, setShowConfirmRevertConfirmRevert] = useState(false);
    const revertConfirmMsg = `Are you sure you wish to revert "${entry.path}" (${entry.type})?`;
    const ref = useRef(null)
    const onSubmit = () => {
        onRevert(entry)
        setShowConfirmRevertConfirmRevert(false)
    };

    return (
        <>
            {onClickShowSummary &&
                <>
                    <OverlayTrigger overlay={<Tooltip>Calculate summary</Tooltip>} placement="bottom">
                        <Button variant="link" ref={ref} style={{visibility: showSummary && "visible"}} /*onBlur={onClickShowSummary}*/ onClick={(e) => {
                            e.preventDefault();
                            onClickShowSummary();
                        }}>
                            <GraphIcon/>
                        </Button>
                    </OverlayTrigger>
                    <Overlay target={ref.current} placement="right" show={showSummary} >
                        {({arrowProps, ...props}) =>
                            <Popover className={"change-summary"} {...props} style={{...props.style}}>
                             <ChangeSummary prefix={entry.path} getMore={getMore}/>
                            </Popover>
                        }
                    </Overlay>
                    &#160;
                </>}
            {entry.type !== 'conflict' && onClickExpandDiff !== null &&
                <><OverlayTrigger placement="bottom"
                                  overlay={<Tooltip>{diffExpanded ? "Hide" : "Show"} changes</Tooltip>}>
                    <Button variant={"link"} disabled={false} style={{visibility: diffExpanded && "visible"}}
                            onClick={(e) => {
                                e.preventDefault();
                                onClickExpandDiff()
                            }}>
                        <FileDiffIcon/>
                    </Button>
                </OverlayTrigger>&#160;</>}
            {onRevert !== null &&
                <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"revert-entry"}>Revert change</Tooltip>)}>
                    <Button variant="link" disabled={false} onClick={(e) => {
                        e.preventDefault();
                        setShowConfirmRevertConfirmRevert(true)
                    }}>
                        <HistoryIcon/>
                    </Button>
                </OverlayTrigger>
            }
            <ConfirmationModal show={showConfirmRevert} onHide={() => setShowConfirmRevertConfirmRevert(false)}
                               msg={revertConfirmMsg} onConfirm={onSubmit}/>
        </>
    );
};

/**
 * Tree item is a node in the tree view. It can be expanded to multiple TreeEntryRow:
 * 1. A single TreeEntryRow for the current prefix (or entry for leaves).
 * 2. Multiple TreeItem as children, each representing another tree node.
 * @param entry The entry the TreeItem is representing, could be either an object or a prefix.
 * @param repo Repository
 * @param reference commitID / branch
 * @param leftDiffRefID commitID / branch
 * @param rightDiffRefID commitID / branch
 * @param internalRefresh to be called when the page refreshes manually
 * @param onRevert to be called when an object/prefix is requested to be reverted
 * @param delimiter objects delimiter ('' or '/')
 * @param after all entries must be greater than after
 * @param relativeTo prefix of the parent item ('' for root elements)
 * @param {(after : string, path : string, useDelimiter :? boolean) => Promise<any>} getMore callback to be called when more items need to be rendered
 */
export const TreeItem = ({
                             entry,
                             repo,
                             reference,
                             leftDiffRefID,
                             rightDiffRefID,
                             internalRefresh,
                             onRevert,
                             onNavigate,
                             delimiter,
                             relativeTo,
                             getMore,
                             depth = 0
                         }) => {
    const [dirExpanded, setDirExpanded] = useState(false); // state of a non-leaf item expansion
    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({results: [], pagination: {}}); // current retrieved children of the item
    const [diffExpanded, setDiffExpanded] = useState(false); // state of a leaf item expansion
    const [showSummary, setShowSummary] = useState(false);
    const {error, loading, nextPage} = useAPIWithPagination(async () => {
        if (!dirExpanded) return
        if (!repo) return

        if (resultsState.results.length > 0 && resultsState.results.at(-1).path > afterUpdated) {
            // results already cached
            return {results: resultsState.results, pagination: resultsState.pagination}
        }

        const {results, pagination} = await getMore(afterUpdated, entry.path)
        setResultsState({results: resultsState.results.concat(results), pagination: pagination})
        return {results: resultsState.results, pagination: pagination}
    }, [repo.id, reference.id, internalRefresh, afterUpdated, entry.path, delimiter, dirExpanded])

    const results = resultsState.results
    if (!!error)
        return <Error error={error}/>

    if (loading && results.length === 0)
        return <TreeEntryRow key={entry.path + "entry-row"} entry={entry} loading={true}
                             relativeTo={relativeTo} depth={depth} onRevert={onRevert} onNavigate={onNavigate}
                             repo={repo} reference={reference} getMore={getMore}/>

    // When the entry represents a tree leaf
    if (!entry.path.endsWith(delimiter))
        return <>
            <TreeEntryRow key={entry.path + "entry-row"} entry={entry} leaf={true}
                          relativeTo={relativeTo} depth={depth === 0 ? 0 : depth + 1} onRevert={onRevert}
                          onNavigate={onNavigate} repo={repo}
                          reference={reference} diffExpanded={diffExpanded}
                          onClickExpandDiff={() => setDiffExpanded(!diffExpanded)} getMore={getMore}/>
            {diffExpanded && <ExpandedLeafRow entry={entry} repoId={repo.id} leftDiffRef={leftDiffRefID}
                                              rightDiffRef={rightDiffRefID} depth={depth} loading={loading}/>
            }
        </>

    return <>
        <TreeEntryRow key={entry.path + "entry-row"} entry={entry} dirExpanded={dirExpanded}
                      relativeTo={relativeTo} depth={depth} onClick={() => setDirExpanded(!dirExpanded)}
                      onRevert={onRevert} onNavigate={onNavigate} repo={repo} reference={reference}
                      showSummary={showSummary} onClickShowSummary={() => setShowSummary(!showSummary)} getMore={getMore}/>
        {dirExpanded && results &&
            results.map(child =>
                (<TreeItem key={child.path + "-item"} entry={child} repo={repo} reference={reference}
                           leftDiffRefID={leftDiffRefID} rightDiffRefID={rightDiffRefID} onRevert={onRevert}
                           onNavigate={onNavigate}
                           internalReferesh={internalRefresh} delimiter={delimiter} depth={depth + 1}
                           relativeTo={entry.path} getMore={getMore}/>))}
        {(!!nextPage || loading) &&
            <TreeEntryPaginator path={entry.path} depth={depth} loading={loading} nextPage={nextPage}
                                setAfterUpdated={setAfterUpdated}/>
        }
    </>
}
/**
 * @param {(after : string, path : string, useDelimiter :? boolean) => Promise<any>} getMore - function to use to get the diff.
 */
export const TreeEntryRow = ({
                                 entry,
                                 relativeTo = "",
                                 leaf = false,
                                 dirExpanded,
                                 diffExpanded,
                                 showSummary,
                                 depth = 0,
                                 onClick,
                                 loading = false,
                                 onRevert,
                                 onNavigate,
                                 onClickExpandDiff = null,
                                 onClickShowSummary = null,
                                 getMore,
                             }) => {
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathSection = extractPathText(entry, relativeTo);
    let diffIndicator = diffIndicatorIcon(entry);
    if (entry.path_type === "common_prefix") {
        pathSection = <Link href={onNavigate(entry)}>{pathSection}</Link>
    }
    return (
        <tr className={rowClass}>
            <td className="diff-indicator">{diffIndicator}</td>
            <td className="tree-path">
                <span style={{marginLeft: (depth * 20 - (leaf ? 10 : 0)) + "px"}}>
                    <span onClick={onClick}>
                        {!leaf && (dirExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>)}
                    </span>
                    {loading ? <ClockIcon/> : ""}
                    {pathSection}
                </span>
            </td>
            <td/>
            <td className={"change-entry-row-actions"}>
                <ChangeRowActions
                    entry={entry}
                    onRevert={onRevert}
                    diffExpanded={diffExpanded}
                    onClickExpandDiff={onClickExpandDiff}
                    showSummary={showSummary}
                    onClickShowSummary={onClickShowSummary}
                    getMore={getMore}
                />
            </td>
        </tr>
    );
};

export const TreeEntryPaginator = ({path, setAfterUpdated, nextPage, depth = 0, loading = false}) => {
    let pathSectionText = "Load more results ...";
    if (path !== "") {
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
                <span style={{marginLeft: depth * 20 + "px", color: "#007bff"}}>
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
            <td className="objects-diff" colSpan={4}>
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
