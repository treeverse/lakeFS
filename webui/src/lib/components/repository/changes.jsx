import React, {useEffect, useState} from "react";

import {
    ClockIcon, DiffIcon, InfoIcon
} from "@primer/octicons-react";

import {useAPI, useAPIWithPagination} from "../../hooks/api";
import {Error, ExperimentalOverlayTooltip} from "../controls";
import {ObjectsDiff} from "./ObjectsDiff";
import {RefTypeBranch, TreeItemType} from "../../../constants";
import * as tablesUtil from "../../../util/tablesUtil";
import {ObjectTreeEntryRow, PrefixTreeEntryRow, TableTreeEntryRow} from "./treeRows";
import Alert from "react-bootstrap/Alert";
import {ComingSoonModal} from "../modals";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import {URINavigator} from "./tree";
import Table from "react-bootstrap/Table";
import {refs, statistics} from "../../api";

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
    const enableDeltaDiff = JSON.parse(localStorage.getItem(`enable_delta_diff`));

    const itemType = useTreeItemType(entry, repo, leftDiffRefID, rightDiffRefID);

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

    if (itemType.loading || (loading && results.length === 0))
        return <ObjectTreeEntryRow key={entry.path+"entry-row"} entry={entry} loading={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} onNavigate={onNavigate} repo={repo} reference={reference}
                                   getMore={getMore}/>

    if (itemType.type === TreeItemType.Object) {
        return <>
            <ObjectTreeEntryRow key={entry.path + "entry-row"} entry={entry} relativeTo={relativeTo}
                                depth={depth === 0 ? 0 : depth + 1} onRevert={onRevert} repo={repo}
                                diffExpanded={diffExpanded} onClickExpandDiff={() => setDiffExpanded(!diffExpanded)}/>
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

    } else if (itemType.type === TreeItemType.Prefix || !enableDeltaDiff) {
        return <>
            <PrefixTreeEntryRow key={entry.path + "entry-row"} entry={entry} dirExpanded={dirExpanded} relativeTo={relativeTo} depth={depth} onClick={() => setDirExpanded(!dirExpanded)} onRevert={onRevert} onNavigate={onNavigate} getMore={getMore} repo={repo} reference={reference}/>
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
    } else {
        return <TableTreeEntryRow key={entry.path + "entry-row"} entry={entry} relativeTo={relativeTo} depth={depth} onRevert={onRevert}/>
    }
}

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

function useTreeItemType(entry, repo, leftDiffRefID, rightDiffRefID) {
    const [treeItemType, setTreeItemType] = useState({type: null, loading: true});

    // Tree items that represent prefixes are always of entry.type = prefix_changed and the actual diff type is
    // presented at the object level. Therefore, in case of tables that were added or removed we don't know
    // under which of the diff refs the table root is expected to be listed and therefore we try to get the table type
    // from both and take the one that returned results.
    let leftResult = useAPI(() => tablesUtil.isDeltaLakeTable(entry, repo, rightDiffRefID));
    let rightResult = useAPI(() => tablesUtil.isDeltaLakeTable(entry, repo, leftDiffRefID));
    useEffect(() => {
        if (entry.path_type === "object") {
            setTreeItemType({ type: TreeItemType.Object, loading: false });
        }
    }, [entry]);
    useEffect(() => {
        if (treeItemType.loading && !leftResult.loading && !rightResult.loading) {
            setTreeItemType({
                type:
                    leftResult.response || rightResult.response
                        ? TreeItemType.DeltaLakeTable
                        : TreeItemType.Prefix,
                loading: false,
            });
        }
    }, [leftResult, rightResult]);
    return treeItemType;
}

export const ChangesTreeContainer = ({results, showExperimentalDeltaDiffButton = false, delimiter, uriNavigator,
                                         leftDiffRefID, rightDiffRefID, repo, reference, internalRefresh, prefix,
                                         getMore, loading, nextPage, setAfterUpdated, onNavigate, onRevert}) => {
    const [showComingSoonModal, setShowComingSoonModal] = useState(false);
    const enableDeltaDiff = JSON.parse(localStorage.getItem(`enable_delta_diff`));
    const sendDeltaDiffStats = async () => {
        const deltaDiffStatEvents = [
            {
                "class": "experimental-feature",
                "name": "delta-diff",
                "count": 1,
            }
        ];
        await statistics.postStatsEvents(deltaDiffStatEvents);
    }

    return <>
        <div className="tree-container">
            {(results.length === 0) ? <Alert variant="info">No changes</Alert> : (
                <>
                    {!enableDeltaDiff ?
                        <>
                        {showExperimentalDeltaDiffButton
                            ? <>
                            <ComingSoonModal display={showComingSoonModal}
                                             onCancel={() => setShowComingSoonModal(false)}>
                                <div>lakeFS Delta Lake tables diff is under development</div>
                            </ComingSoonModal>
                            <ExperimentalOverlayTooltip>
                                <Button className="action-bar"
                                        variant="primary"
                                        disabled={false}
                                        onClick={async () => {
                                            setShowComingSoonModal(true);
                                            await sendDeltaDiffStats();
                                        }}>
                                    <DiffIcon/> Compare Delta Lake tables
                                </Button>
                            </ExperimentalOverlayTooltip>
                            </>
                            : ""}</>
                        :
                        <div className="mr-1 mb-2">
                            <Alert variant={"info"}><InfoIcon/> You can now use lakeFS to compare Delta Lake tables</Alert>
                        </div>
                    }
                    <Card>
                        <Card.Header>
                                <span className="float-start">
                                    {(delimiter !== "") && uriNavigator}
                                </span>
                        </Card.Header>
                        <Card.Body>
                            <Table borderless size="sm">
                                <tbody>
                                {results.map(entry => {
                                    return (
                                        <TreeItem key={entry.path + "-item"} entry={entry} repo={repo}
                                                  reference={reference}
                                                  internalReferesh={internalRefresh} leftDiffRefID={leftDiffRefID}
                                                  rightDiffRefID={rightDiffRefID} delimiter={delimiter}
                                                  relativeTo={prefix}
                                                  onNavigate={onNavigate}
                                                  getMore={getMore}
                                                  onRevert={onRevert}
                                                 />);
                                })}
                                {!!nextPage &&
                                <TreeEntryPaginator path={""} loading={loading} nextPage={nextPage}
                                                    setAfterUpdated={setAfterUpdated}/>}
                                </tbody>
                            </Table>
                        </Card.Body>
                    </Card></>
            )}
        </div>
    </>
}
