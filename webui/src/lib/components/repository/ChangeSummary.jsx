import React, {useCallback, useEffect, useState} from "react";
import {
    ClockIcon,
    DiffAddedIcon,
    DiffIgnoredIcon,
    DiffModifiedIcon,
    DiffRemovedIcon,
} from "@primer/octicons-react";
import {OverlayTrigger, Tooltip} from "react-bootstrap";
import {humanSize} from "./tree";

const MAX_NUM_OBJECTS = 10_000;
const PAGE_SIZE = 1_000;

class SummaryEntry {
    constructor() {
        this.count = 0
        this.sizeBytes = 0
    }
    add(count, sizeBytes) {
        this.count += count
        this.sizeBytes += sizeBytes
    }
}

class SummaryData {
    constructor() {
        this.added = new SummaryEntry()
        this.changed = new SummaryEntry()
        this.removed = new SummaryEntry()
        this.conflict = new SummaryEntry()
    }
}

/**
 * Widget to display a summary of a change: the number of added/changed/deleted/conflicting objects.
 * Shows an error if the change has more than {@link MAX_NUM_OBJECTS} entries.

 * @param {string} prefix - prefix to display summary for.
 * @param {(after : string, path : string, useDelimiter :? boolean, amount :? number) => Promise<any> } getMore - function to use to get the change entries.
 */
export default ({prefix, getMore}) => {
    const [pullMore, setPullMore] = useState(false);
    const [resultsState, setResultsState] = useState({results: [], pagination: {}});
    const [loading, setLoading] = useState(true);
    useEffect(() => {
        const calculateChanges = async () => {
            // get pages until reaching the max change size
            if (resultsState.results && resultsState.results.length >= MAX_NUM_OBJECTS && !pullMore) {
                setLoading(false)
                return
            }
            if (!loading) {
                return
            }
            const {results, pagination} = await getMore(resultsState.pagination.next_offset || "", prefix, false, PAGE_SIZE)
            if (!pagination.has_more) {
                setLoading(false)
            }
            setResultsState({results: resultsState.results.concat(results), pagination: pagination})
        }

        calculateChanges()
            .catch(e => {
                alert(e.toString());
                setResultsState({results: [], pagination: {}})
                setLoading(false)
            })
    }, [resultsState.results, loading, pullMore])

    const onLoadAll = useCallback((e) => {
        e.preventDefault()
        setLoading(true)
        setPullMore(true)
    }, [setLoading, setPullMore])

    if (loading || !resultsState || !resultsState.results) return <ClockIcon/>
    if (resultsState.results && resultsState.results.length >= MAX_NUM_OBJECTS && !pullMore) {
        return (
            <OverlayTrigger placement="bottom"
                            overlay={
                                <Tooltip>
                                   <span className={"small font-weight-bold"}>
                                       Can&apos;t show summary for a change with more than {MAX_NUM_OBJECTS} objects
                                   </span>
                                </Tooltip>
                            }>
                <small>
                    &gt;= {MAX_NUM_OBJECTS.toLocaleString()} results, <a href="#" onClick={onLoadAll}>load more?</a>
                </small>
            </OverlayTrigger>
        )
    }
    const summaryData = resultsState.results.reduce((prev, current) => {
        prev[current.type].add(1, current.size_bytes)
        return prev
    }, new SummaryData())
    const detailsTooltip = <Tooltip>
        <div className="m-1 small text-start">
            {summaryData.added.count > 0 &&
                <><span className={"color-fg-added"}>{summaryData.added.count.toLocaleString()}</span> objects added (total {humanSize(summaryData.added.sizeBytes)})<br/></>}
            {summaryData.removed.count > 0 &&
                <><span className={"color-fg-removed"}>{summaryData.removed.count.toLocaleString()}</span> objects removed (total {humanSize(summaryData.removed.sizeBytes)})<br/></>}
            {summaryData.changed.count > 0 &&
                <><span className={"color-fg-changed"}>{summaryData.changed.count.toLocaleString()}</span> objects changed<br/></>}
            {summaryData.conflict.count > 0 &&
                <><span className={"color-fg-conflict"}>{summaryData.conflict.count.toLocaleString()}</span> conflicts<br/></>}
        </div>
    </Tooltip>
    return (
        <OverlayTrigger placement="left" overlay={detailsTooltip}>
            <div className={"m-1 small float-end"}>
                {summaryData.added.count > 0 &&
                    <span className={"color-fg-added"}><DiffAddedIcon className={"change-summary-icon"}/>{summaryData.added.count.toLocaleString()}</span>}
                {summaryData.removed.count > 0 &&
                    <span className={"color-fg-removed"}><DiffRemovedIcon className={"change-summary-icon"}/>{summaryData.removed.count.toLocaleString()}</span>}
                {summaryData.changed.count > 0 &&
                    <span className={"font-weight-bold"}><DiffModifiedIcon className={"change-summary-icon"}/>{summaryData.changed.count.toLocaleString()}</span>}
                {summaryData.conflict.count > 0 &&
                    <span className={"color-fg-conflict"}><DiffIgnoredIcon className={"change-summary-icon"}/>{summaryData.conflict.count.toLocaleString()}</span>}
            </div>
        </OverlayTrigger>
    )
}
