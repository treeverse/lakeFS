import React, {useEffect, useState, useCallback} from "react";
import {useHistory, useLocation} from "react-router-dom";
import {connect} from "react-redux";
import {Alert, ButtonToolbar, Button, OverlayTrigger, Tooltip} from "react-bootstrap";
import {SyncIcon, GitMergeIcon} from "@primer/octicons-react";
import {diff, diffPaginate, merge, resetMerge} from "../actions/refs";
import RefDropdown from "./RefDropdown";
import Changes from "./Changes";
import ConfirmationModal from "./ConfirmationModal";
import {PAGINATION_AMOUNT} from "../actions/refs";

const MergeButton = connect(
    ({ refs }) => ({
        mergeState: refs.merge,
        diffResults: refs.diff,
    }),
    ({ merge, resetMerge })
)(({ repo, refId, compare, merge, mergeState, resetMerge, diffResults }) => {
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
    } else if (diffItems.some(x => x.type === 'conflict')) {
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
            // TODO(barak): test if we need to reset and refresh diff after merge?!
        // } else if (mergeState.payload && mergeState.payload.results.length > 0) {
        //     resetDiff();
        }
    }, [resetMerge, mergeState]);

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
                    <GitMergeIcon /> Merge
                </Button>
            </span>
        </OverlayTrigger>
        </>
    );
});

const CompareToolbar = ({repo, refId, compare, refresh}) => {
    const history = useHistory();
    const location = useLocation();

    return  (
        <>
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
        </ButtonToolbar>
        <ButtonToolbar className="float-right mb-2">
            <OverlayTrigger placement="bottom" overlay={<Tooltip id="refreshTooltipId">Refresh</Tooltip>}>
                <Button variant="light" onClick={refresh}><SyncIcon/></Button>
            </OverlayTrigger>
            <MergeButton repo={repo} refId={refId} compare={compare} />
        </ButtonToolbar>
        </>
    );
};

const ComparePage = ({repo, refId, compareRef, diff, diffPaginate, diffResults, resetMerge, mergeResults }) => {
    const refreshData = useCallback(() => {
        if (compareRef) {
            diff(repo.id, refId.id, compareRef.id);
        }
    }, [repo.id, refId.id, diff, compareRef]);

    useEffect(() => {
        refreshData();
    }, [refreshData, repo.id, refId.id, diff, diffPaginate, compareRef]);

    const paginator =(!diffResults.loading && !!diffResults.payload && diffResults.payload.pagination && diffResults.payload.pagination.has_more);
    const showMergeCompleted = !!(mergeResults && mergeResults.payload);
    const compareWith = !compareRef || (compareRef && refId.type === compareRef.type && refId.id === compareRef.id);
    const alertText = diffResults.error || '';
    return (
        <div className="mt-3">
            <div className="action-bar">
                <CompareToolbar refId={refId} repo={repo} compare={compareRef} refresh={refreshData}/>
            </div>

            <Alert variant="warning" show={compareWith}>
                <Alert.Heading>There isn’t anything to compare.</Alert.Heading>
                You’ll need to use two different sources to get a valid comparison.
            </Alert>

            <Alert variant="success" show={showMergeCompleted} onClick={() => resetMerge()} dismissible>
                Merge completed
            </Alert>

            <Alert variant="danger" show={!!alertText}>
                <Alert.Heading>{alertText}</Alert.Heading>
            </Alert>

            {!(compareWith || alertText) &&
                <>
                <Changes
                    repo={repo}
                    refId={refId}
                    showActions={false}
                    list={diffResults}
                    />

                {paginator &&
                <p className="tree-paginator">
                    <Button variant="outline-primary" onClick={() => {
                        diffPaginate(repo.id, refId.id, compareRef.id, diffResults.payload.pagination.next_offset, PAGINATION_AMOUNT);
                    }}>
                        Load More
                    </Button>
                </p>
                }
                </>
            }
        </div>
    );
};

export default connect(
    ({ refs }) => ({
        diffResults: refs.diff,
        mergeResults: refs.merge,
    }),
    ({ diff, diffPaginate, resetMerge })
)(ComparePage);
