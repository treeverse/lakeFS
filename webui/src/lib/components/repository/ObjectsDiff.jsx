import React from "react";
import {useAPI} from "../../hooks/api";
import {objects} from "../../api";
import ReactDiffViewer, {DiffMethod} from "react-diff-viewer";
import {Error, Loading} from "../controls";

export const ObjectsDiff = ({diffType, repoId, leftRef, rightRef, path}) => {
    switch (diffType) {
        case 'changed':
            return <ChangedDiff repoId={repoId} leftRef={leftRef} rightRef={rightRef} path={path}/>;
        case 'added':
            return <AddedDiff repoId={repoId} rightRef={rightRef} path={path}/>;
        case 'removed':
            return <RemovedDiff repoId={repoId} leftRef={leftRef} path={path}/>;
        case 'conflict':
            return <ConflictDiff path={path}/>;
        default:
            return <Error error={"Unsupported diff type " + diffType}/>;
    }
}

// github shows 20KB diff according to https://github.blog/2016-12-06-how-we-made-diff-pages-3x-faster/
const maxDiffSizeBytes = 20000;
const supportedReadableFormats = ["txt", "csv", "tsv"];

function readableObject(path) {
    for (let i = 0; i < supportedReadableFormats.length; i++) {
        if (path.endsWith(supportedReadableFormats[i])) {
            return true;
        }
    }
    return false;
}

const ConflictDiff = ({path}) => {
    return "Conflict in " + path + " resolve the conflict manually to review objects diff.";
}

const ChangedDiff =  ({repoId, leftRef, rightRef, path}) => {
    const readable = readableObject(path);
    const getFn = readable ? objects.getWithSizeConstraint : objects.getStat;
    const left = useAPI(async () => getFn(repoId, leftRef, path, maxDiffSizeBytes),
        [repoId, leftRef, path]);
    const right = useAPI(async () => getFn(repoId, rightRef, path, maxDiffSizeBytes),
        [repoId, rightRef, path]);

    if (left.loading || right.loading) return <Loading/>;
    if (!!right.error) return <Error error={right.error}/>;
    if (!!left.error) return <Error error={left.error}/>;

    return readable
        ? <ContentDiff left={left.response[0]} right={right.response[0]} leftSize={(left.response[1]).get("Content-length")}
                       rightSize={(right.response[1]).get("Content-length")} diffType={"changed"}/>
        : <StatDiff left={left.response} right={right.response} diffType={"changed"}/>;
}

const AddedDiff =  ({repoId, rightRef, path}) => {
    let readable = readableObject(path);
    const getFn = readable ? objects.getWithSizeConstraint : objects.getStat;
    const right = useAPI(async () => getFn(repoId, rightRef, path, maxDiffSizeBytes),
            [repoId, rightRef, path]);

    if (right.loading) return <Loading/>;
    if (!!right.error) return <Error error={right.error}/>;

    return readable
        ? <ContentDiff right={right.response[0]} rightSize={(right.response[1]).get("Content-length")} diffType={"added"}/>
        : <StatDiff right={right.response} diffType={"added"}/>;
}

const RemovedDiff =  ({repoId, leftRef, path}) => {
    let readable = readableObject(path);
    const getFn = readable ? objects.getWithSizeConstraint : objects.getStat;
    const left = useAPI(async () => getFn(repoId, leftRef, path, maxDiffSizeBytes),
            [repoId, leftRef, path]);

    if (left.loading) return <Loading/>;
    if (!!left.error) return <Error error={left.error}/>;

    return readable
        ? <ContentDiff left={left.response[0]} leftSize={(left.response[1]).get("Content-length")} diffType={"removed"}/>
        : <StatDiff left={left.response} diffType={"removed"}/>;
}

const ContentDiff = ({left, right, leftSize, rightSize, diffType}) => {
    const newStyles = {
        variables: {
            light: {
                codeFoldGutterBackground: "#6F767E",
                codeFoldBackground: "#E2E4E5"
            }
        }
    };

    return <div>
        <span><StatsReportBlock leftSize={leftSize} rightSize={rightSize} diffType={diffType}/></span>
        <ReactDiffViewer
            oldValue={left}
            newValue={right}
            splitView={false}
            styles={newStyles}
            compareMethod={DiffMethod.LINES}/>
    </div>;
}


const StatDiff = ({left, right, diffType}) => {
    let statsReportBlock;
    if (diffType === 'changed') {
        if (typeof left === 'undefined' || typeof right === 'undefined') {
            return <Error error={"Invalid input"}/>;
        }
        statsReportBlock = <StatsReportBlock leftSize={left.size_bytes} rightSize={right.size_bytes} diffType={diffType}/>;
    } else if (diffType === 'added') {
        if (typeof right === 'undefined') {
            return <Error error={"Invalid input: right is undefined"}/>;
        }
        statsReportBlock = <StatsReportBlock rightSize={right.size_bytes} diffType={diffType}/>;
    } else {
        if (typeof left === 'undefined') {
            return <Error error={"Invalid input: left is undefined"}/>;
        }
        statsReportBlock = <StatsReportBlock leftSize={left.size_bytes} diffType={diffType}/>;
    }

    return <>
        <div className={"stats-diff-block"}>
            {statsReportBlock}
        </div>
    </>;
}

const StatsReportBlock = ({leftSize, rightSize, diffType}) => {
    let msgDiv;
    if (diffType === 'changed') {
        let sizeDiff = leftSize - rightSize;
        if (sizeDiff === 0) {
            return <Error error={"Diff size cannot be 0, otherwise the objects were not different"}/>;
        }
        if (sizeDiff < 0) {
            msgDiv = <div>
                <span style={{color: "#339933", fontWeight: "bold"}}>added </span>
                <span style={{fontWeight: "bold"}}>{ Math.abs(sizeDiff) + "B"}</span>
                <span> in size</span>
            </div>;
        } else {
            msgDiv = <div>
                <span style={{color: "#FF3300", fontWeight: "bold"}}>removed </span>
                <span style={{fontWeight: "bold"}}>{ Math.abs(sizeDiff) + "B"}</span>
                <span> in size</span>
            </div>;
        }
    } else if (diffType === 'added') {
        msgDiv = <div>
            <span style={{color: "#339933", fontWeight: "bold"}}>added </span>
            <span style={{fontWeight: "bold"}}>{rightSize + "B"}</span>
            <span> in size</span>
        </div>;
    } else {
        msgDiv = <div>
            <span style={{color: "#FF3300", fontWeight: "bold"}}>removed</span>
            <span style={{fontWeight: "bold"}}>{leftSize + "B"}</span>
            <span> in size</span>
        </div>;
    }
    return msgDiv;
}
