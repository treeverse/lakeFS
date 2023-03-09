import React from "react";
import {useAPI} from "../../hooks/api";
import {objects} from "../../api";
import ReactDiffViewer, {DiffMethod} from "react-diff-viewer-continued";
import {Error, Loading} from "../controls";
import {humanSize} from "./tree";
import Alert from "react-bootstrap/Alert";

const maxDiffSizeBytes = 120 << 10;
const supportedReadableFormats = ["txt", "csv", "tsv"];

export const ObjectsDiff = ({diffType, repoId, leftRef, rightRef, path}) => {
    const readable = readableObject(path);
    let left;
    let right;
    switch (diffType) {
        case 'changed':
        case 'conflict':
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
        default:
            return <Error error={"Unsupported diff type " + diffType}/>;
    }

    if ((left && left.loading) || (right && right.loading)) return <Loading/>;
    const err = (left && left.error) || (right && right.err);
    if (err) return <Error error={err}/>;

    const leftStat = left && left.response;
    const rightStat = right && right.response;
    if (!readable) {
        return <NoContentDiff left={leftStat} right={rightStat} diffType={diffType}/>;
    }
    const objectTooBig = (leftStat && leftStat.size_bytes > maxDiffSizeBytes) || (rightStat && rightStat.size_bytes > maxDiffSizeBytes);
    if (objectTooBig) {
        return <Error error={path + " is too big (> " + humanSize(maxDiffSizeBytes)+ "). To view its diff please download" +
        " the objects and use an external diff tool."}/>
    }
    const leftSize = leftStat && leftStat.size_bytes;
    const rightSize = rightStat && rightStat.size_bytes;
    return <ContentDiff repoId={repoId} path={path} leftRef={left && leftRef} rightRef={right && rightRef}
                        leftSize={leftSize} rightSize={rightSize} diffType={diffType}/>;
}

function readableObject(path) {
    for (const ext of supportedReadableFormats) {
        if (path.endsWith("." + ext)) {
            return true;
        }
    }
    return false;
}

const NoContentDiff = ({left, right, diffType}) => {
    return <div>
        <span><StatDiff left={left} right={right} diffType={diffType}/></span>
        <span><Alert variant="warning">lakeFS supports content diff for .tsv, .csv, and .txt file formats only</Alert></span>
    </div>;
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
            compareMethod={DiffMethod.WORDS}/>
    </div>;
}

function validateDiffInput(left, right, diffType) {
    switch (diffType) {
        case 'changed':
            if (!left && !right) return <Error error={"Invalid diff input"}/>;
            break;
        case 'added':
            if (!right) return <Error error={"Invalid diff input: right hand-side is missing"}/>;
            break;
        case 'removed':
            if (!left) return <Error error={"Invalid diff input: left hand-side is missing"}/>;
            break;
        case 'conflict':
            break;
        default:
            return <Error error={"Unknown diff type: " + diffType}/>;
    }
}

const StatDiff = ({left, right, diffType}) => {
    const err = validateDiffInput(left, right, diffType);
    if (err) return err;
    const rightSize = right && right.size_bytes;
    const leftSize = left && left.size_bytes;
    return <>
        <div className={"stats-diff-block"}>
            <DiffSizeReport leftSize={leftSize} rightSize={rightSize} diffType={diffType}/>
        </div>
    </>;
}

const DiffSizeReport = ({leftSize, rightSize, diffType}) => {
    let label = diffType;
    let size;
    switch (diffType) {
        case 'changed':
            size = leftSize - rightSize;
            if (size === 0) {
                return <div>
                    <span className="unchanged">identical file size</span>
                </div>;
            }
            if (size < 0) {
                size = -size;
                label = "added";
            } else {
                label = "removed";
            }
            break;
        case 'conflict': // conflict will compare left and right. further details: https://github.com/treeverse/lakeFS/issues/3269
                return <div>
                    <span className={label}>{label} </span>
                    <span>both source and destination file were changed.</span>
                    <span className={"diff-size"}> Source: {humanSize(leftSize)}</span>
                    <span> in size, </span>
                    <span className={"diff-size"}> Destination: {humanSize(rightSize)}</span>
                    <span> in size</span>
                </div>;
        case 'added':
            size = rightSize;
            break;
        case 'removed':
            size = leftSize;
            break;
        default:
            return <Error error={"Unknown diff type: " + diffType}/>;
    }


    return <div>
        <span className={label}>{label} </span>
        <span className={"diff-size"}>{humanSize(size)}</span>
        <span> in size</span>
    </div>;
}
