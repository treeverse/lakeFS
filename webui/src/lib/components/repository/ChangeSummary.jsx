import React, {useState} from "react";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import {useAPIWithPagination} from "../../hooks/api";
import {Loading} from "../controls";

/**
 * @param {string} prefix - prefix to display summary for.
 * @param {(after : string, path : string, useDelimiter :? boolean) => Promise<any> } getMore - function to use to get the change entries.
 */
export default ({prefix, getMore}) => {
    const [resultsState, setResultsState] = useState({results: [], pagination:{}});
    const { loading, nextPage } = useAPIWithPagination(async () => {
        const {results, pagination} = await getMore("", prefix, false)
        setResultsState({results: resultsState.results.concat(results), pagination: pagination})
    })
    console.log(JSON.stringify(resultsState.results))
    if (loading) return <Loading/>
    if (nextPage) {
        return "Diff too big"
    }
    const diffSummary = resultsState.results.reduce((prev, current) => {
        prev[current.type][0]++
        prev[current.type][1] += current.size_bytes
        return prev
    }, {added: [0, 0], removed: [0, 0], changed: [0, 0], conflict: [0, 0]})
    return <Container>
        <Row className={"m-1"}>
            {diffSummary.added[0] > 0 && <><span className={"color-fg-added"}>+{diffSummary.added[0]}</span>&#160;objects (total {diffSummary.added[1]}B)<br/></>}
            {diffSummary.removed[0] > 0 && <><span className={"color-fg-removed"}>-{diffSummary.removed[0]}</span>&#160;objects</>}
            {diffSummary.changed[0] > 0 && <>{diffSummary.changed[0]} changed objects</>}
            {diffSummary.conflict[0] > 0 && <>{diffSummary.conflict[0]} conflicts</>}
        </Row>
    </Container>
}
