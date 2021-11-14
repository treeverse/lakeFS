import React from "react";
import {useAPI} from "../../hooks/api";
import {objects} from "../../api";
import ReactDiffViewer, {DiffMethod} from "react-diff-viewer";
import {Error, Loading} from "../controls";
import {humanSize} from "./tree";

// github shows 20KB diff according to https://github.blog/2016-12-06-how-we-made-diff-pages-3x-faster/
const maxDiffSizeBytes = 20 << 10;
const supportedReadableFormats = ["txt", "csv", "tsv"];

export const ObjectsDiff = ({diffType, repoId, leftRef, rightRef, path}) => {
    switch (diffType) {
        case 'changed':
            return <ChangedDiff repoId={repoId} leftRef={leftRef} rightRef={rightRef} path={path}/>;
        case 'added':
            return <AddedDiff repoId={repoId} rightRef={rightRef} path={path}/>;
        case 'removed':
            return <RemovedDiff repoId={repoId} leftRef={leftRef} path={path}/>;
        case 'conflict':
            return <>{path} has a conflict. Please resolve the conflict manually and then review objects diff.</>;
        default:
            return <Error error={"Unsupported diff type " + diffType}/>;
    }
}

function readableObject(path) {
    for (const ext of supportedReadableFormats) {
        if (path.endsWith("." + ext)) {
            return true;
        }
    }
    return false;
}

const ChangedDiff = ({repoId, leftRef, rightRef, path}) => {
    const readable = readableObject(path);
    const getFn = readable ? objects.getWithSizeConstraint : objects.getStat;
    const left = useAPI(async () => getFn(repoId, leftRef, path, maxDiffSizeBytes),
        [repoId, leftRef, path]);
    const right = useAPI(async () => getFn(repoId, rightRef, path, maxDiffSizeBytes),
        [repoId, rightRef, path]);

    if (left.loading || right.loading) return <Loading/>;
    const err = right.error || left.error;
    if (err) return <Error error={err}/>;

    if (readable) {
        const leftBody = left.response[0];
        const leftSize = (left.response[1]).get("Content-length");
        const rightBody = right.response[0];
        const rightSize = (right.response[1]).get("Content-length");
        return <ContentDiff left={leftBody} right={rightBody} leftSize={leftSize} rightSize={rightSize} diffType={"changed"}/>;
    }
    return <StatDiff left={left.response} right={right.response} diffType={"changed"}/>;
}

const AddedDiff = ({repoId, rightRef, path}) => {
    let readable = readableObject(path);
    const getFn = readable ? objects.getWithSizeConstraint : objects.getStat;
    const right = useAPI(async () => getFn(repoId, rightRef, path, maxDiffSizeBytes),
            [repoId, rightRef, path]);

    if (right.loading) return <Loading/>;
    if (right.error) return <Error error={right.error}/>;

    if (readable) {
        const rightBody = right.response[0];
        const rightSize = (right.response[1]).get("Content-length");
        return <ContentDiff right={rightBody} rightSize={rightSize} diffType={"added"}/>;
    }
    return <StatDiff right={right.response} diffType={"added"}/>;
}

const RemovedDiff = ({repoId, leftRef, path}) => {
    let readable = readableObject(path);
    const getFn = readable ? objects.getWithSizeConstraint : objects.getStat;
    const left = useAPI(async () => getFn(repoId, leftRef, path, maxDiffSizeBytes),
            [repoId, leftRef, path]);

    if (left.loading) return <Loading/>;
    if (left.error) return <Error error={left.error}/>;

    if (readable) {
        const leftBody = left.response[0];
        const leftSize = (left.response[1]).get("Content-length");
        return <ContentDiff left={leftBody} leftSize={leftSize} diffType={"removed"}/>;
    }
    return <StatDiff left={left.response} diffType={"removed"}/>;
}

const ContentDiff = ({left, right, leftSize, rightSize, diffType}) => {
    return <div>
        <span><DiffSizeReport leftSize={leftSize} rightSize={rightSize} diffType={diffType}/></span>
        <ReactDiffViewer
            oldValue={left}
            newValue={right}
            splitView={false}
            compareMethod={DiffMethod.LINES}/>
    </div>;
}


const StatDiff = ({left, right, diffType}) => {
    switch (diffType) {
        case 'changed':
            if (!left && !right) return <Error error={"Invalid input"}/>;
            break;
        case 'added':
            if (!right) return <Error error={"Invalid input: right is missing"}/>;
            break;
        case 'removed':
            if (!left) return <Error error={"Invalid input: left is missing"}/>;
            break;
        default:
            return <Error error={"Unknown diff type: " + diffType}/>;
    }
    const rightSize = right && right.size_bytes;
    const leftSize = left && left.size_bytes;
    return <>
        <div className={"stats-diff-block"}>
            <DiffSizeReport leftSize={leftSize} rightSize={rightSize} diffType={diffType}/>
        </div>
    </>;
}

const DiffSizeReport = ({leftSize, rightSize, diffType}) => {
    let diffVerbSpan;
    let size;
    switch (diffType) {
        case 'changed':
            let sizeDiff = leftSize - rightSize;
            if (sizeDiff < 0) {
                size = humanSize(Math.abs(sizeDiff));
                diffVerbSpan = <span className={"added"}>added </span>
            } else {
                size = humanSize(sizeDiff);
                diffVerbSpan = <span className={"removed"}>removed </span>
            }
            break;
        case 'added':
            size = humanSize(rightSize)
            diffVerbSpan = <span className={"added"}>added </span>
            break;
        case 'removed':
            size = humanSize(leftSize)
            diffVerbSpan =  <span className={"removed"}>removed </span>
            break;
        default:
            return <Error error={"Unknown diff type: " + diffType}/>;
            break;
    }
    return <div>
                {diffVerbSpan}
                <span className={"diff-size"}>{size}</span>
                <span> in size</span>
            </div>;
}
