import React, {useState} from "react";

import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
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
import {useAPI, useAPIWithPagination} from "../../hooks/api";
import {Error} from "../controls";
import {ObjectsDiff} from "./ObjectsDiff";
import {ConfirmationModal} from "../modals";
import ChangeSummary from "./ChangeSummary";
import {ChangesTreeType} from "../../../constants";
import {objects} from "../../api";

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
const ChangeRowActions = ({actions}) => <>
    {
        actions.map(action => (
            <><OverlayTrigger placement="bottom" overlay={<Tooltip>{action.tooltip}</Tooltip>}>
                <Link className={"btn-link"} disabled={false} style={{visibility: action.visible ? "visible" : ""}}
                        onClick={(e) => {
                            e.preventDefault();
                            action.onClick()
                        }}>
                    {action.icon}
                </Link>
            </OverlayTrigger>&#160;&#160;</>
        ))}
</>;

/**
 * Tree item is a node in the tree view. It can be expanded to multiple TreeEntryRow:
 * 1. A single TreeEntryRow for the current prefix (or entry for leaves).
 * 2. Multiple TreeItem as children, each representing another tree node.
 * @param entry The entry the TreeItem is representing, could be either an object or a prefix.
 * @param repo Repository
 * @param changesTreeType the type of changes tree the tree item is created for - object changes or table changes
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
export const TreeItem = ({ entry, repo, changesTreeType = ChangesTreeType.ObjectChanges, reference, leftDiffRefID, rightDiffRefID, internalRefresh, onRevert, onNavigate, delimiter, relativeTo, getMore, depth=0 }) => {
    const [dirExpanded, setDirExpanded] = useState(false); // state of a non-leaf item expansion
    const [afterUpdated, setAfterUpdated] = useState(""); // state of pagination of the item's children
    const [resultsState, setResultsState] = useState({results:[], pagination:{}}); // current retrieved children of the item
    const [objectDiffExpanded, setObjectDiffExpanded] = useState(false); // state of a leaf item expansion
    const [deltaLakeDiffExpanded, setDeltaLakeDiffExpanded] = useState(false); // state of a leaf item expansion

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

    let isDeltaTableRootEntry = false
    if (changesTreeType === ChangesTreeType.DeltaLakeTableChanges) {
        // The delta tables diff container only shows prefixes.
        if (entry.path_type === "object") {
            return
        }

        // The delta tables diff container only shows prefixes that are either delta tables or prefixes that can
        // potentially lead to delta tables. The later are prefixes that have prefixes under them as opposed to only
        // have objects.

        // Tree items that represent prefixes are always of entry.type = prefix_changed and the actual diff type is
        // presented at the object level. Therefore, in case of delta tables that were added or removed we don't know
        // under which of the diff refs the prefix is expected to be listed and therefore we list both and take the one
        // that returned results.
        const listRight = useAPI(() => objects.list(repo.id, rightDiffRefID, entry.path), []);
        const listLeft = useAPI(() => objects.list(repo.id, leftDiffRefID, entry.path), []);
        let listPrefix = listRight.response !== null && listRight.response.results.length !== 0 ? listRight : listLeft
        if (listPrefix.response !== null && listPrefix.response.results.length !== 0) {
            let listedItems = listPrefix.response.results;
            let isPotentialRouteToDeltaTable = false;
            for (let i = 0; i < listedItems.length; i++) {
                let item = listedItems[i];
                if (item.path_type === "common_prefix") {
                    isPotentialRouteToDeltaTable = true;
                }
                if (item.path.includes("_delta_log/")) {
                    isDeltaTableRootEntry = true;
                    break;
                }
            }
            if (!isPotentialRouteToDeltaTable) {
                return;
            }
        }
    }

    const results = resultsState.results
    if (error)
        return <Error error={error}/>

    if (loading && results.length === 0)
        return <TreeEntryRow key={entry.path+"entry-row"} entry={entry} loading={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} onNavigate={onNavigate} repo={repo} reference={reference}
                             getMore={getMore}/>

    // When the entry represents a tree leaf
    if (!entry.path.endsWith(delimiter))
        return <>
            <TreeEntryRow key={entry.path + "entry-row"} entry={entry} leaf={true} changesTreeType={changesTreeType} relativeTo={relativeTo} depth={depth === 0 ? 0 : depth + 1} onRevert={onRevert} onNavigate={onNavigate} repo={repo}
                          reference={reference} diffExpanded={objectDiffExpanded} onClickExpandObjectDiff={() => setObjectDiffExpanded(!objectDiffExpanded)} getMore={getMore}/>
            {objectDiffExpanded && <tr key={"row-" + entry.path} className={"leaf-entry-row"}>
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
        <TreeEntryRow key={entry.path + "entry-row"} entry={entry} changesTreeType={changesTreeType} dirExpanded={dirExpanded} relativeTo={relativeTo} depth={depth} onClick={() => setDirExpanded(!dirExpanded)} onRevert={onRevert} onNavigate={onNavigate} repo={repo} reference={reference} getMore={getMore}
                      isDeltaTableRootEntry={isDeltaTableRootEntry} onClickExpandDeltaLakeDiff={() => setDeltaLakeDiffExpanded(!deltaLakeDiffExpanded)} deltaLakeDiffExpanded={deltaLakeDiffExpanded}/>
        {dirExpanded && results &&
            results.map(child =>
                (<TreeItem key={child.path + "-item"} entry={child} repo={repo} changesTreeType={changesTreeType} reference={reference} leftDiffRefID={leftDiffRefID} rightDiffRefID={rightDiffRefID} onRevert={onRevert} onNavigate={onNavigate}
                           internalReferesh={internalRefresh} delimiter={delimiter} depth={depth + 1}
                           relativeTo={entry.path} getMore={getMore}/>))}
        {(!!nextPage || loading) &&
            <TreeEntryPaginator path={entry.path} depth={depth} loading={loading} nextPage={nextPage}
                                setAfterUpdated={setAfterUpdated}/>
        }
    </>
}

export const TreeEntryRow = ({entry, relativeTo = "", leaf = false, changesTreeType = ChangesTreeType.ObjectChanges, dirExpanded, diffExpanded, depth = 0, onClick, loading = false, onRevert, onNavigate, onClickExpandObjectDiff = null, onClickExpandDeltaLakeDiff = null, isDeltaTableRootEntry = false, deltaLakeDiffExpanded,  getMore}) => {
    const [showRevertConfirm, setShowRevertConfirm] = useState(false)
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathSection = extractPathText(entry, relativeTo);
    let diffIndicator = diffIndicatorIcon(entry);
    const [showSummary, setShowSummary] = useState(false);
    if (entry.path_type === "common_prefix") {
        pathSection = <Link href={onNavigate(entry)}>{pathSection}</Link>
    }
    const rowActions = []
    switch (changesTreeType) {
        case ChangesTreeType.ObjectChanges:
            if (onClickExpandObjectDiff) {
                rowActions.push(new RowAction(<FileDiffIcon/>, diffExpanded ? "Hide changes" : "Show changes", diffExpanded, onClickExpandObjectDiff))
            }
            if (!leaf) {
                rowActions.push(new RowAction(<GraphIcon/>, showSummary ? "Hide summary" : "Calculate change summary", showSummary, () => setShowSummary(!showSummary)))
            }
            if (onRevert) {
                rowActions.push(new RowAction(<HistoryIcon/>, "Revert changes", false, () => {
                    setShowRevertConfirm(true)
                }))
            }
            break;
        case ChangesTreeType.DeltaLakeTableChanges:
            if (isDeltaTableRootEntry) {
                rowActions.push(new RowAction(<FileDiffIcon/>, deltaLakeDiffExpanded ? "Hide table changes" : "Show table changes", deltaLakeDiffExpanded, onClickExpandDeltaLakeDiff))
            }
            break;
        default:
            throw new Error('Unsupported ChangesTreeType');
    }
    return (
        <tr className={rowClass}>
            <td className="pl-4 col-auto p-2">{diffIndicator}</td>
            <td className="col-9 tree-path">
                <span style={{marginLeft: (depth * 20) + "px"}}>
                    <span onClick={onClick}>
                        {!isDeltaTableRootEntry && !leaf && (dirExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>)}
                    </span>
                    {loading ? <ClockIcon/> : ""}
                    {pathSection}
                </span>
            </td>
            <td className={"col-2 p-0 text-right"}>{showSummary && <ChangeSummary prefix={entry.path} getMore={getMore}/>}</td>
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

export const DeltaTableChangesTreeItem = ({ entry, repo, reference, leftDiffRefID, rightDiffRefID, internalRefresh, onRevert, onNavigate, delimiter, relativeTo, getMore}) => {
    return <>
        <TreeItem key={entry.path + "-item"} entry={entry} repo={repo}
                  changesTreeType={ChangesTreeType.DeltaLakeTableChanges}
                  reference={reference}
                  internalReferesh={internalRefresh} leftDiffRefID={leftDiffRefID}
                  rightDiffRefID={rightDiffRefID} delimiter={delimiter}
                  relativeTo={relativeTo}
                  onNavigate={onNavigate}
                  getMore={getMore}/>
    </>
}

export const ObjectChangesTreeItem = ({ entry, repo, reference, leftDiffRefID, rightDiffRefID, internalRefresh, onRevert, onNavigate, delimiter, relativeTo, getMore}) => {
    return <>
        <TreeItem key={entry.path + "-item"} entry={entry} repo={repo}
                  reference={reference}
                  internalReferesh={internalRefresh} leftDiffRefID={leftDiffRefID}
                  rightDiffRefID={rightDiffRefID} delimiter={delimiter}
                  relativeTo={relativeTo}
                  onNavigate={onNavigate}
                  getMore={getMore}/>
    </>
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
