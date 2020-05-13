import React, {useEffect, useState} from "react";
import {useHistory, useLocation} from "react-router-dom";
import {connect} from "react-redux";

import ButtonToolbar from "react-bootstrap/ButtonToolbar";
import Button from "react-bootstrap/Button";

import Octicon, {GitMerge} from "@primer/octicons-react";

import {PAGINATION_AMOUNT, listTree, listTreePaginate} from "../actions/objects";
import {diff, resetDiff, merge, resetMerge} from "../actions/refs";
import RefDropdown from "./RefDropdown";
import Tree from "./Tree";
import ConfirmationModal from "./ConfirmationModal";
import Alert from "react-bootstrap/Alert";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";

const readUncommitted = false;

const MergeButton = connect(
    ({ refs }) => ({
        mergeState: refs.merge,
        diffResults: refs.diff,
    }),
    ({ merge, resetMerge, resetDiff })
)(({ repo, refId, compare, merge, mergeState, resetMerge, resetDiff, diffResults }) => {
    if (!refId || refId.type !== 'branch' || !compare || compare.type !== 'branch') {
        return null;
    }

    const diffItems = diffResults.payload ? diffResults.payload.results : [];
    const destinationBranchId = compare.id;
    const sourceBranchId = refId.id;
    let mergeDisabled = true;
    let mergeVariant = 'light';
    let mergeText;
    if (destinationBranchId === sourceBranchId) {
        mergeText = 'Please select a different branch to compare with';
    } else if (diffItems.length === 0) {
        mergeText = `No changes found between '${sourceBranchId}' and '${destinationBranchId}'`;
    } else if (diffItems.some(x => x.direction === 'CONFLICT')) {
        mergeText = `Conflict found between '${sourceBranchId}' and '${destinationBranchId}'`;
    } else {
        mergeText = `Merge '${sourceBranchId}' into '${destinationBranchId}'`;
        mergeDisabled = false;
        mergeVariant = 'success';
    }

    const [show, setShow] = useState(false);

    const disabled = mergeState.inProgress;

    const onHide = () => {
        if (disabled) return;
        setShow(false);
    };
    
    useEffect(() => {
        if (mergeState.error) {
            window.alert(mergeState.error);
            resetMerge();
        } else if (mergeState.payload && mergeState.payload.results.length > 0) {
            resetDiff();
        }
    }, [resetMerge, mergeState, resetDiff]);

    const onSubmit = () => {
        if (disabled) return;
        merge(repo.id, sourceBranchId, destinationBranchId);
        setShow(false);
    };

    return (
        <>
        <ConfirmationModal show={show} onHide={onHide} msg={mergeText} onConfirm={onSubmit} />
        <OverlayTrigger placement="bottom" overlay={<Tooltip id="mergeTextTooltip">{mergeText}</Tooltip>}>
            <span>
                <Button variant={mergeVariant}
                    disabled={mergeDisabled}
                    style={mergeDisabled ? { pointerEvents: "none" } : {}}
                    onClick={() => { resetMerge(); setShow(true); }}>
                    <Octicon icon={GitMerge} /> Merge
                </Button>
            </span>
        </OverlayTrigger>
        </>
    );
});

const CompareToolbar = ({repo, refId, compare}) => {
    const history = useHistory();
    const location = useLocation();

    return  (
        <ButtonToolbar className="float-left mb-2">

            <RefDropdown
                repo={repo}
                selected={refId}
                withWorkspace={false}
                selectRef={(ref) => {
                const params = new URLSearchParams(location.search);
                if (ref.type === 'branch') {
                    params.set('branch', ref.id);
                    params.delete('commit'); // if we explicitly selected a branch, remove an existing commit if any
                } else {
                    params.set('commit', ref.id);
                    params.delete('branch'); // if we explicitly selected a commit, remove an existing branch if any
                }

                params.delete('compareCommit');
                params.delete('compareBranch');
                history.push({...location, search: params.toString()})
            }}/>

            <RefDropdown
                repo={repo} 
                selected={compare}
                prefix={'Compared to '}
                emptyText={'Compare with...'}
                withWorkspace={false}
                onCancel={() => {
                    const params = new URLSearchParams(location.search);
                    params.delete('compareBranch');
                    params.delete('compareCommit');
                    history.push({...location, search: params.toString()})
                }}
                selectRef={(ref) => {
                    const params = new URLSearchParams(location.search);
                    if (ref.type === 'branch') {
                        params.set('compareBranch', ref.id);
                        params.delete('compareCommit'); // if we explicitly selected a branch, remove an existing commit if any
                    } else {
                        params.set('compareCommit', ref.id);
                        params.delete('compareBranch'); // if we explicitly selected a commit, remove an existing branch if any
                    }
                    history.push({...location, search: params.toString()})
                }}/>

            <MergeButton repo={repo} refId={refId} compare={compare} />

        </ButtonToolbar>
    );
};

const ComparePage = ({repo, refId, compareRef, path, list, listTree, listTreePaginate, diff, resetDiff, diffResults, resetMerge, mergeResults }) => {
    const history = useHistory();
    const location = useLocation();

    useEffect(() => {
        listTree(repo.id, refId.id, path, PAGINATION_AMOUNT, readUncommitted);
    }, [repo.id, refId.id, path, listTree]);

    useEffect(() => {
        if (compareRef) {
            diff(repo.id, refId.id, compareRef.id);
        } else {
            resetDiff();
        }
    }, [repo.id, refId, diff, resetDiff, compareRef]);

    let paginator = (<span/>);
    if (!list.loading && !!list.payload && list.payload.pagination && list.payload.pagination.has_more) {
        paginator = (
            <p className="tree-paginator">
                <Button variant="outline-primary" onClick={() => {
                    listTreePaginate(repo.id, refId.id, path, list.payload.pagination.next_offset, PAGINATION_AMOUNT, readUncommitted);
                }}>
                    Load More
                </Button>
            </p>
        );
    }

    const showMergeCompleted = !!(mergeResults && mergeResults.payload);
    return (
        <div className="mt-3">
            <Alert variant="success" show={showMergeCompleted} onClick={() => resetMerge()} dismissible>
                Merge completed
            </Alert>
            <div className="action-bar">
                <CompareToolbar refId={refId} repo={repo} compare={compareRef}/>
            </div>

            <Tree
                repo={repo}
                refId={refId}
                showActions={false}
                onNavigate={(path) => {
                    const params = new URLSearchParams(location.search);
                    params.set('path', path);
                    history.push({...location, search: params.toString()});
                }}
                diffResults={diffResults}
                list={list}
                path={path}/>

            {paginator}
        </div>
    );
};

export default connect(
    ({ objects, refs }) => ({
        list: objects.list,
        diffResults: refs.diff,
        mergeResults: refs.merge,
    }),
    ({ listTree, listTreePaginate, diff, resetDiff, resetMerge })
)(ComparePage);
