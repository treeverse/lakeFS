import React, {useCallback, useEffect, useState} from "react";

import {
    ArrowLeftIcon,
    ClockIcon, DiffIcon, InfoIcon, PlusIcon, XIcon
} from "@primer/octicons-react";

import {useAPI, useAPIWithPagination} from "../../hooks/api";
import {Error, ExperimentalOverlayTooltip} from "../controls";
import {ObjectsDiff} from "./ObjectsDiff";
import {TreeItemType} from "../../../constants";
import * as tablesUtil from "../../../util/tablesUtil";
import {ObjectTreeEntryRow, PrefixTreeEntryRow, TableTreeEntryRow} from "./treeRows";
import Alert from "react-bootstrap/Alert";
import {ComingSoonModal} from "../modals";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import {refs, statistics} from "../../api";
import {DeltaLakeDiff} from "./TableDiff";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

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
export const TreeItemRow = ({ entry, repo, reference, leftDiffRefID, rightDiffRefID, internalRefresh, onRevert, onNavigate, delimiter, relativeTo, getMore,
                                depth=0, setTableDiffExpanded}) => {
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
        return <tr><td><Error error={error}/></td></tr>

    if (itemType.loading || (loading && results.length === 0))
        return <ObjectTreeEntryRow key={entry.path+"entry-row"} entry={entry} loading={true} relativeTo={relativeTo} depth={depth} onRevert={onRevert} repo={repo} reference={reference}
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
                (<TreeItemRow key={child.path + "-item"} entry={child} repo={repo} reference={reference} leftDiffRefID={leftDiffRefID} rightDiffRefID={rightDiffRefID} onRevert={onRevert} onNavigate={onNavigate}
                              internalReferesh={internalRefresh} delimiter={delimiter} depth={depth + 1}
                              relativeTo={entry.path} getMore={getMore} setTableDiffExpanded={setTableDiffExpanded}/>))}
            {(!!nextPage || loading) &&
            <TreeEntryPaginator path={entry.path} depth={depth} loading={loading} nextPage={nextPage}
                                setAfterUpdated={setAfterUpdated}/>
        }
    </>
    } else {
        return <TableTreeEntryRow key={entry.path + "entry-row"} entry={entry} relativeTo={relativeTo} depth={depth} onRevert={onRevert} onClickExpandDiff={setTableDiffExpanded}/>
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

/**
 * A container component for entries that represent a diff between refs. This container is used by the compare, commit changes,
 * and uncommitted changes views.
 *
 * @param results to be displayed in the changes tree container
 * @param showExperimentalDeltaDiffButton whether or not to display a delta-specific experimental feature button. TODO (Tals): remove when enabling the delta diff feature.
 * @param delimiter objects delimiter ('' or '/')
 * @param uriNavigator to navigate in the page using the changes container
 * @param leftDiffRefID commitID / branch
 * @param rightDiffRefID commitID / branch
 * @param repo Repository
 * @param reference commitID / branch
 * @param internalRefresh to be called when the page refreshes manually
 * @param prefix for which changes are displayed
 * @param getMore to be called when requesting more diff results for a prefix
 * @param loading of API response state to get changes
 * @param nextPage of API response state to get changes
 * @param setAfterUpdated state of pagination of the item's children
 * @param onNavigate to be called when navigating to a prefix
 * @param onRevert to be called when an object/prefix is requested to be reverted
 */
export const ChangesTreeContainer = ({results, showExperimentalDeltaDiffButton = false, delimiter, uriNavigator,
                                         leftDiffRefID, rightDiffRefID, repo, reference, internalRefresh, prefix,
                                         getMore, loading, nextPage, setAfterUpdated, onNavigate, onRevert, setIsTableMerge}) => {
    const enableDeltaDiff = JSON.parse(localStorage.getItem(`enable_delta_diff`));
    const [tableDiffState, setTableDiffState] = useState({isShown: false, expandedTablePath: ""});

    if (results.length === 0) {
        return <div className="tree-container">
            <Alert variant="info">No changes</Alert>
        </div>
    } else {
        return <div className="tree-container">
                    {!enableDeltaDiff
                        ? <ExperimentalDeltaDiffButton showButton={showExperimentalDeltaDiffButton}/>
                        : tableDiffState.isShown
                                ? <Button className="action-bar"
                                          variant="secondary"
                                          disabled={false}
                                          onClick={() => {
                                              setTableDiffState( {isShown: false, expandedTablePath: ""})
                                              if (setIsTableMerge) {
                                                  setIsTableMerge(false);
                                              }
                                          }}>
                                    <ArrowLeftIcon/> Back to object comparison
                                  </Button>
                                : <div className="mr-1 mb-2"><Alert variant={"info"}><InfoIcon/> You can now use lakeFS to
                                    compare Delta Lake tables</Alert></div>
                    }
                    <Card>
                        <Card.Header>
                                <span className="float-start">
                                    {(delimiter !== "") && uriNavigator}
                                </span>
                        </Card.Header>
                        <Card.Body>
                            {tableDiffState.isShown
                                ? <DeltaLakeDiff repo={repo} leftRef={leftDiffRefID} rightRef={rightDiffRefID} tablePath={tableDiffState.expandedTablePath}/>
                                : <Table borderless size="sm">
                                <tbody>
                                {results.map(entry => {
                                    return (
                                        <TreeItemRow key={entry.path + "-item"} entry={entry} repo={repo}
                                                     reference={reference}
                                                     internalReferesh={internalRefresh} leftDiffRefID={leftDiffRefID}
                                                     rightDiffRefID={rightDiffRefID} delimiter={delimiter}
                                                     relativeTo={prefix}
                                                     onNavigate={onNavigate}
                                                     getMore={getMore}
                                                     onRevert={onRevert}
                                                     setTableDiffExpanded={() => {
                                                         setTableDiffState({isShown: true,  expandedTablePath: entry.path})
                                                         if (setIsTableMerge) {
                                                             setIsTableMerge(true);
                                                         }
                                                     }}
                                                 />);
                                })}
                                {!!nextPage &&
                                <TreeEntryPaginator path={""} loading={loading} nextPage={nextPage}
                                                    setAfterUpdated={setAfterUpdated}/>}
                                </tbody>
                            </Table>}
                        </Card.Body>
                    </Card>
            </div>
    }
}

export const defaultGetMoreChanges = (repo, leftRefId, rightRefId, delimiter) => (afterUpdated, path, useDelimiter= true, amount = -1) => {
    return refs.diff(repo.id, leftRefId, rightRefId, afterUpdated, path, useDelimiter ? delimiter : "", amount > 0 ? amount : undefined);
};

const ExperimentalDeltaDiffButton = ({showButton = false}) => {
    const [showComingSoonModal, setShowComingSoonModal] = useState(false);
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
        <ComingSoonModal display={showComingSoonModal}
                         onCancel={() => setShowComingSoonModal(false)}>
            <div>lakeFS Delta Lake tables diff is under development</div>
        </ComingSoonModal>
        <ExperimentalOverlayTooltip>
            <Button className="action-bar"
                    variant="primary"
                    hidden={!showButton}
                    onClick={async () => {
                        setShowComingSoonModal(true);
                        await sendDeltaDiffStats();
                    }}>
                <DiffIcon/> Compare Delta Lake tables
            </Button>
        </ExperimentalOverlayTooltip>
    </>
}

export const MetadataFields = ({ metadataFields, setMetadataFields}) => {
    const onChangeKey = useCallback((i) => {
        return e => {
            console.log(e)
            const key = e.currentTarget.value;
            setMetadataFields(prev => [...prev.slice(0,i), {...prev[i], key}, ...prev.slice(i+1)]);
            e.preventDefault()
        };
    }, [setMetadataFields]);

    const onChangeValue = useCallback((i) => {
        return e => {
            const value = e.currentTarget.value;
            setMetadataFields(prev => [...prev.slice(0,i),  {...prev[i], value}, ...prev.slice(i+1)]);
        };
    }, [setMetadataFields]);

    const onRemovePair = useCallback((i) => {
        return () => setMetadataFields(prev => [...prev.slice(0, i), ...prev.slice(i + 1)])
    }, [setMetadataFields])

    const onAddPair = useCallback(() => {
        setMetadataFields(prev => [...prev, {key: "", value: ""}])
    }, [setMetadataFields])

    return (
        <div className="mt-3 mb-3">
            {metadataFields.map((f, i) => {
                return (
                    <Form.Group key={`commit-metadata-field-${i}`} className="mb-3">
                        <Row>
                            <Col md={{span: 5}}>
                                <Form.Control type="text" placeholder="Key" defaultValue={f.key} onChange={onChangeKey(i)}/>
                            </Col>
                            <Col md={{span: 5}}>
                                <Form.Control type="text" placeholder="Value" defaultValue={f.value}  onChange={onChangeValue(i)}/>
                            </Col>
                            <Col md={{span: 1}}>
                                <Form.Text>
                                    <Button size="sm" variant="secondary" onClick={onRemovePair(i)}>
                                        <XIcon/>
                                    </Button>
                                </Form.Text>
                            </Col>
                        </Row>
                    </Form.Group>
                )
            })}
            <Button onClick={onAddPair} size="sm" variant="secondary">
                <PlusIcon/>{' '}
                Add Metadata field
            </Button>
        </div>
    )
}