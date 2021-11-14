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
    const readable = readableObject(path);
    let left;
    let right;
    switch (diffType) {
        case 'changed':
            left = useAPI(async () => objects.getStat(repoId, leftRef, path),
                [repoId, leftRef, path]);
            right = useAPI(async () => objects.getStat(repoId, rightRef, path),
                [repoId, rightRef, path]);
            break;
        case 'added':
            right = useAPI(async () => objects.getStat(repoId, rightRef, path),
                [repoId, rightRef, path]);
            break;
        case 'removed':
            left = useAPI(async () => objects.getStat(repoId, leftRef, path),
                [repoId, leftRef, path]);
            break;
        case 'conflict':
            return <>{path} has a conflict. Please resolve the conflict manually and then review objects diff.</>;
            break;
        default:
            return <Error error={"Unsupported diff type " + diffType}/>;
    }

    if ((left && left.loading) || (right && right.loading)) return <Loading/>;
    const err = (left && left.error) || (right && right.err);
    if (err) return <Error error={err}/>;

    if (readable) {
        if ((!left || left.response.size_bytes <= maxDiffSizeBytes) && (!right || right.response.size_bytes <= maxDiffSizeBytes)) {
            const leftSize = left && left.response.size_bytes;
            const rightSize = right && right.response.size_bytes;
            return <ContentDiff repoId={repoId} path={path} leftRef={left && leftRef} rightRef={right && rightRef}
                                leftSize={left && leftSize} rightSize={right && rightSize} diffType={diffType}/>
        }
        return <Error error={path + " is too big (> 20KB). To view its diff please download the objects and use an " +
            "external diff tool."}/>
    }
    return <StatDiff left={left && left.response} right={right && right.response} diffType={diffType}/>;
}

function readableObject(path) {
    for (const ext of supportedReadableFormats) {
        if (path.endsWith("." + ext)) {
            return true;
        }
    }
    return false;
}

const ContentDiff = ({repoId, path, leftRef, rightRef, leftSize, rightSize, diffType}) => {
    const left = leftRef && useAPI(async () => objects.get(repoId, leftRef, path),
        [repoId, leftRef, path]);
    const right = rightRef && useAPI(async () => objects.get(repoId, rightRef, path),
        [repoId, rightRef, path]);

    if ((left && left.loading) || (right && right.loading)) return <Loading/>;
    const err = (left && left.error) || (right && right.err);
    if (err) return <Error error={err}/>;

    return <div>
        <span><DiffSizeReport leftSize={leftSize} rightSize={rightSize} diffType={diffType}/></span>
        <ReactDiffViewer
            oldValue={left && left.response}
            newValue={right && right.response}
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
