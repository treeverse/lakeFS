import React, {useState, Fragment} from "react";
import OverlayTrigger from 'react-bootstrap/OverlayTrigger';
import Tooltip from 'react-bootstrap/Tooltip';
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

import {Link} from "../nav";
import {useAPIWithPagination} from "../../hooks/api";
import {Error} from "../controls";
import {ObjectsDiff} from "./ObjectsDiff";
import {ConfirmationModal} from "../modals";
import ChangeSummary from "./ChangeSummary";

class RowAction {
    /**
     * @param {JSX.Element} icon
     * @param {string} tooltip
     * @param {boolean} visible
     * @param {()=>void} onClick
     */
    constructor(icon, tooltip, visible, onClick) {
        this.tooltip = tooltip
        this.visible = visible
        this.onClick = onClick
        this.icon = icon
    }
}

/**
 * @param {[RowAction]} actions
 */
const ChangeRowActions = ({actions}) => {
    const actionFragments = actions.map((action, i) => (
        <Fragment key={`action-${i}`}>
            <OverlayTrigger placement="bottom" overlay={
                <Tooltip id="tooltip-action-${i}">
                    {action.tooltip}
                </Tooltip>
            }>
                <span>
                    <Link className={"btn-link"} disabled={false} style={{visibility: action.visible ? "visible" : ""}}
                          onClick={e => {
                              e.preventDefault();
                              action.onClick()
                          }}>
                        {action.icon}
                    </Link>
                </span>
            </OverlayTrigger>&#160;&#160;
        </Fragment>
    ));

    return (
        <>
            {actionFragments}
        </>
    );
}

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
 * @param {(after : string, path : string, useDelimiter :? boolean, amount :? number) => Promise<any> } getMore callback to be called when more items need to be rendered
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
    if (error)
        return <Error error={error}/>

    if (loading && results.length === 0)
        return <TreeEntryRow key={entry.path+"entry-row"} entry={entry} loading={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} onNavigate={onNavigate} repo={repo} reference={reference}
                             getMore={getMore}/>

    // When the entry represents a tree leaf
    if (!entry.path.endsWith(delimiter))
        return <>
            <TreeEntryRow key={entry.path + "entry-row"} entry={entry} leaf={true} relativeTo={relativeTo} depth={depth === 0 ? 0 : depth + 1} onRevert={onRevert} onNavigate={onNavigate} repo={repo}
                          reference={reference} diffExpanded={diffExpanded} onClickExpandDiff={() => setDiffExpanded(!diffExpanded)} getMore={getMore}/>
            {diffExpanded && <tr key={"row-" + entry.path} className={"leaf-entry-row"}>
                <td className="objects-diff" colSpan={4}>
                    <ObjectsDiff
                        diffType={entry.type}
                        repoId={repo.id}
                        leftRef={leftDiffRefID}
                        rightRef={rightDiffRefID}
                        path={entry.path}
                    />
            {loading && <ClockIcon/>}
                </td>
                </tr>
            }
        </>

    return <>
        <TreeEntryRow key={entry.path + "entry-row"} entry={entry} dirExpanded={dirExpanded} relativeTo={relativeTo} depth={depth} onClick={() => setDirExpanded(!dirExpanded)} onRevert={onRevert} onNavigate={onNavigate} repo={repo} reference={reference} getMore={getMore}/>
        {dirExpanded && results &&
            results.map(child =>
                (<TreeItem key={child.path + "-item"} entry={child} repo={repo} reference={reference} leftDiffRefID={leftDiffRefID} rightDiffRefID={rightDiffRefID} onRevert={onRevert} onNavigate={onNavigate}
                           internalReferesh={internalRefresh} delimiter={delimiter} depth={depth + 1}
                           relativeTo={entry.path} getMore={getMore}/>))}
        {(!!nextPage || loading) &&
            <TreeEntryPaginator path={entry.path} depth={depth} loading={loading} nextPage={nextPage}
                                setAfterUpdated={setAfterUpdated}/>
        }
    </>
}

export const TreeEntryRow = ({entry, relativeTo = "", leaf = false, dirExpanded, diffExpanded, depth = 0, onClick, loading = false, onRevert, onNavigate, onClickExpandDiff = null, getMore}) => {
    const [showRevertConfirm, setShowRevertConfirm] = useState(false)
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathSection = extractPathText(entry, relativeTo);
    let diffIndicator = diffIndicatorIcon(entry);
    const [showSummary, setShowSummary] = useState(false);
    if (entry.path_type === "common_prefix") {
        pathSection = <Link href={onNavigate(entry)}>{pathSection}</Link>
    }
    const rowActions = []
    if (onClickExpandDiff) {
        rowActions.push(new RowAction(<FileDiffIcon/>, diffExpanded ? "Hide changes" : "Show changes", diffExpanded, onClickExpandDiff))
    }
    if (!leaf) {
        rowActions.push(new RowAction(<GraphIcon/>, showSummary ? "Hide summary" : "Calculate change summary", showSummary, () => setShowSummary(!showSummary)))
    }
    if (onRevert) {
        rowActions.push(new RowAction(<HistoryIcon/>, "Revert changes", false, () => {
            setShowRevertConfirm(true)
        }))
    }
    return (
        <tr className={rowClass}>
            <td className="pl-4 col-auto p-2">{diffIndicator}</td>
            <td className="col-9 tree-path">
                <span style={{marginLeft: (depth * 20) + "px"}}>
                    <span onClick={onClick}>
                        {!leaf && (dirExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>)}
                    </span>
                    {loading ? <ClockIcon/> : ""}
                    {pathSection}
                </span>
            </td>
            <td className={"col-2 p-0 text-end"}>{showSummary && <ChangeSummary prefix={entry.path} getMore={getMore}/>}</td>
            <td className={"col-1 change-entry-row-actions"}>
                <ChangeRowActions actions={rowActions} />
                <ConfirmationModal show={showRevertConfirm} onHide={() => setShowRevertConfirm(false)}
                                   msg={`Are you sure you wish to revert "${entry.path}" (${entry.type})?`}
                                   onConfirm={() => onRevert(entry)}/>
            </td>
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
            onClick={() => setAfterUpdated(nextPage)}
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
        case 'prefix_changed':
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
        return <OverlayTrigger placement="bottom" overlay={(<Tooltip id={"tooltip-prefix"}>Changes under prefix</Tooltip>)}>
                        <span>
                            <FileDirectoryIcon/>
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
