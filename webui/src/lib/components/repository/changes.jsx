import React, {useCallback, useEffect, useRef, useState} from "react";

import {
    ClockIcon, FileDirectoryFillIcon,
    FoldDownIcon,
    FoldUpIcon,
    PlusIcon,
    XIcon
} from "@primer/octicons-react";

import {useAPIWithPagination} from "../../hooks/api";
import {useExpandCollapseDirs} from "./useExpandCollapseDirs";
import {AlertError} from "../controls";
import {ObjectsDiff} from "./ObjectsDiff";
import {ObjectTreeEntryRow, PrefixTreeEntryRow} from "./treeRows";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import {refs} from "../../api";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

/**
 * Tree item is a node in the tree view. It can be expanded to multiple TreeEntryRow:
 *   1. A single TreeEntryRow for the current prefix (or entry for leaves).
 *   2. Multiple TreeItem as children, each representing another tree node.
 * Also, it syncs with global "Expand All / Collapse All" via `expandMode`.
 * Ignores global mode if the user manually toggled the folder.
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
                                depth=0, onToggleDir, expandMode, registerDir, markDirAsManuallyToggled, tick }) => {

    // Tracks whether this specific folder was manually toggled by the user.
    // Used to ignore global expandMode changes once the user interacts manually.
    const userToggledRef = useRef(false);

    const [dirExpanded, setDirExpanded] = useState(false);
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

    useEffect(() => {
        if (entry.path_type !== "object") {
            registerDir(entry.path);
        }
    }, [entry.path_type, entry.path, registerDir]);

    useEffect(() => {
        // When the global expand/collapse mode changes (triggered by the button),
        // update this specific directory's expanded state — unless the user manually changed it.
        if (entry.path_type !== "object" && expandMode && expandMode.value !== null) {
            userToggledRef.current = false; // Mark as "not manually toggled" so sync with global mode is allowed
            setDirExpanded(expandMode.value);
            onToggleDir(entry.path, expandMode.value);
        }
    }, [expandMode?.version]);

    const results = resultsState.results
    if (error)
        return <tr><td><AlertError error={error}/></td></tr>

    if (loading && results.length === 0) {
        return <ObjectTreeEntryRow key={entry.path + "entry-row"} entry={entry} loading={true} relativeTo={relativeTo}
                                   depth={depth} onRevert={onRevert} repo={repo} reference={reference}
                                   getMore={getMore}/>
    }
    if (entry.path_type === "object") {
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
    }
    // entry is a common prefix
    return <>
        <PrefixTreeEntryRow key={entry.path + "entry-row"} entry={entry} dirExpanded={dirExpanded} relativeTo={relativeTo} depth={depth}
                            onClick={() => {
                                userToggledRef.current = true;
                                markDirAsManuallyToggled(entry.path);
                                setDirExpanded(!dirExpanded);
                                onToggleDir(entry.path, !dirExpanded);
                            }}
                            onRevert={onRevert} onNavigate={onNavigate} getMore={getMore} repo={repo} reference={reference}/>
        {dirExpanded && results &&
        results.map(child =>
            (<TreeItemRow key={child.path + "-item"} entry={child} repo={repo} reference={reference} leftDiffRefID={leftDiffRefID} rightDiffRefID={rightDiffRefID} onRevert={onRevert} onNavigate={onNavigate}
                          internalReferesh={internalRefresh} delimiter={delimiter} depth={depth + 1}
                          relativeTo={entry.path} getMore={getMore}
                          onToggleDir={onToggleDir}
                          expandMode={userToggledRef.current ? undefined : expandMode}
                          registerDir={registerDir}
                          markDirAsManuallyToggled={markDirAsManuallyToggled}
                          tick={tick}
                          />))}
        {(!!nextPage || loading) &&
        <TreeEntryPaginator path={entry.path} depth={depth} loading={loading} nextPage={nextPage}
                            setAfterUpdated={setAfterUpdated}/>
    }
    </>
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

/**
 * A container component for entries that represent a diff between refs. This container is used by the compare, commit changes,
 * and uncommitted changes views.
 *
 * @param results to be displayed in the changes tree container
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
 * @param changesTreeMessage
 */
export const ChangesTreeContainer = ({results, delimiter, uriNavigator,
                                         leftDiffRefID, rightDiffRefID, repo, reference, internalRefresh, prefix,
                                         getMore, loading, nextPage, setAfterUpdated, onNavigate, onRevert,
                                         changesTreeMessage}) => {
    // Manages expand/collapse state for all directories in the tree.
    const { allDirsExpanded, expandMode, toggleAllDirs, updateOpenedDir, registerDir, markDirAsManuallyToggled, tick } = useExpandCollapseDirs();
    if (results.length === 0) {
        return <div className="tree-container">
            <Alert variant="info">No changes</Alert>
        </div>
    } else {
        return <div className="tree-container">
            {changesTreeMessage && <div>{changesTreeMessage}</div>}
                    <Card>
                        <Card.Header className="d-flex justify-content-between align-items-center">
                            {(delimiter !== "") && uriNavigator}
                            {changesTreeMessage && (
                                <Button
                                    size="sm"
                                    variant="outline-secondary"
                                    onClick={toggleAllDirs}
                                    title={allDirsExpanded ? "Collapse all" : "Expand all"}
                                >
                                    <FileDirectoryFillIcon className="me-1" />
                                    {allDirsExpanded ? <FoldUpIcon /> : <FoldDownIcon />}
                                </Button>
                            )}
                        </Card.Header>
                        <Card.Body>
                            <Table borderless size="sm">
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
                                                     onToggleDir={updateOpenedDir}
                                                     expandMode={expandMode}
                                                     registerDir={registerDir}
                                                     markDirAsManuallyToggled={markDirAsManuallyToggled}
                                                     tick={tick}
                                                 />);
                                })}
                                {!!nextPage &&
                                <TreeEntryPaginator path={""} loading={loading} nextPage={nextPage}
                                                    setAfterUpdated={setAfterUpdated}/>}
                                </tbody>
                            </Table>
                        </Card.Body>
                    </Card>
            </div>
    }
}

export const defaultGetMoreChanges = (repo, leftRefId, rightRefId, delimiter) => (afterUpdated, path, useDelimiter= true, amount = -1) => {
    return refs.diff(repo.id, leftRefId, rightRefId, afterUpdated, path, useDelimiter ? delimiter : "", amount > 0 ? amount : undefined);
};

export const MetadataFields = ({ metadataFields, setMetadataFields, ...rest}) => {
    const onChangeKey = useCallback((i) => {
        return e => {
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
        <div className="mt-3 mb-3" {...rest}>
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
