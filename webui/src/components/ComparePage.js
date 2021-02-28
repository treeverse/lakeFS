import React, {useEffect, useState, useCallback} from "react";
import {useHistory, useLocation} from "react-router-dom";
import {connect} from "react-redux";
import {Alert, ButtonToolbar, Button, OverlayTrigger, Tooltip} from "react-bootstrap";
import {SyncIcon, GitMergeIcon, ArrowLeftIcon} from "@primer/octicons-react";
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
    const destinationBranchId = refId.id;
    const sourceBranchId = compare.id;
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
    
    // useEffect(() => {
    //     if (mergeState.error) {
            // window.alert(mergeState.error);
            // resetMerge();
            // TODO(barak): test if we need to reset and refresh diff after merge?!
        // } else if (mergeState.payload && mergeState.payload.results.length > 0) {
        //     resetDiff();
        // }
    // }, [resetMerge, mergeState]);

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
                prefix={'Base '}
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
            <ArrowLeftIcon className="mr-2 mt-2"/>
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
        resetMerge();
        if (compareRef && (compareRef.id !== refId.id)) {
            diff(repo.id, compareRef.id, refId.id);
        }
    }, [repo.id, refId.id, diff, compareRef, resetMerge]);

    useEffect(() => {
        refreshData();
    }, [refreshData, repo.id, refId.id, diff, diffPaginate, compareRef]);

    const getRunID = (err) => {
        if (!err) {
            return '';
        }
        const m = /^Error: (\S+) hook aborted, run id '([^']+)'/.exec(err);
        if (!m) {
            return '';
        }
        return m[2];
    };

    const formatAlertText = (err) => {
        if (!err) {
            return '';
        }
        const lines = err.split('\n');
        if (lines.length === 1) {
            return <Alert.Heading>{err}</Alert.Heading>;
        }
        const runID = getRunID(err);
        return lines.map((line, i) => {
            if (runID) {
                const m = /^\t\* hook run id '([^']+)' failed/.exec(line);
                if (m) {
                    const hookRunID = m[1];
                    const link = `/api/v1/repositories/${repo.id}/actions/runs/${runID}/hooks/${hookRunID}/output`;
                    return <p key={i}><Alert.Link target="_blank" download={hookRunID} href={link}>{line}</Alert.Link></p>;
                }
            }
            return <p key={i}>{line}</p>;
        });
    };

    const formatMoreInformation = (err) => {
        const runID = getRunID(err);
        if (!runID) {
            return '';
        }
        const cmd = `lakectl actions runs describe lakefs://${repo.id} ${runID}`;
        return <><p>More information by running:</p>{cmd}</>;
    };

    const paginator =(!diffResults.loading && !!diffResults.payload && diffResults.payload.pagination && diffResults.payload.pagination.has_more);
    const showMergeCompleted = !!(mergeResults && mergeResults.payload);
    const compareWith = !compareRef || (refId.type === compareRef.type && refId.id === compareRef.id);
    const alertText = formatAlertText(diffResults.error || mergeResults.error);
    const alertMoreInformation = formatMoreInformation(mergeResults.error);
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
                {alertText}
                {alertMoreInformation &&
                    (<>
                        <hr/>
                        {alertMoreInformation}
                    </>)
                }
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
                        diffPaginate(repo.id, compareRef.id, refId.id, diffResults.payload.pagination.next_offset, PAGINATION_AMOUNT);
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
