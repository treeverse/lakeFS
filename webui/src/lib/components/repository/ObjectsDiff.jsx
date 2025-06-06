import React, {useContext} from "react";
import {useAPI} from "../../hooks/api";
import {objects} from "../../api";
import ReactDiffViewer, {DiffMethod} from "react-diff-viewer-continued";
import {AlertError, Loading} from "../controls";
import {humanSize} from "./tree";
import Alert from "react-bootstrap/Alert";
import {InfoIcon} from "@primer/octicons-react";
import {useStorageConfigs} from "../../hooks/storageConfig";
import {AppContext} from "../../hooks/appContext";
import {useRefs} from "../../hooks/repo";
import {getRepoStorageConfig} from "../../../pages/repositories/repository/utils";

const maxDiffSizeBytes = 120 << 10;
const supportedReadableFormats = ["txt", "text", "md", "csv", "tsv", "yaml", "yml", "json", "jsonl", "ndjson", "geojson"];

export const ObjectsDiff = ({diffType, repoId, leftRef, rightRef, path}) => {
    const {repo, error: refsError, loading: refsLoading} = useRefs();
    const {configs: storageConfigs, error: configsError, loading: storageConfigsLoading} = useStorageConfigs();
    const {storageConfig, error: storageConfigError} = getRepoStorageConfig(storageConfigs, repo);
    const hooksLoading = refsLoading || storageConfigsLoading;
    const hooksError = hooksLoading ? null : refsError || configsError || storageConfigError;

    if (hooksError) return <AlertError error={hooksError}/>;

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
            return <AlertError error={"Unsupported diff type " + diffType}/>;
    }

    if (hooksLoading || (left && left.loading) || (right && right.loading)) return <Loading/>;
    const err = (left && left.error) || (right && right.err);
    if (err) return <AlertError error={err}/>;

    const leftStat = left && left.response;
    const rightStat = right && right.response;
    if (!readable) {
        return <NoContentDiff left={leftStat} right={rightStat} diffType={diffType}/>;
    }
    const objectTooBig = (leftStat && leftStat.size_bytes > maxDiffSizeBytes) || (rightStat && rightStat.size_bytes > maxDiffSizeBytes);
    if (objectTooBig) {
        return <AlertError error={path + " is too big (> " + humanSize(maxDiffSizeBytes)+ "). To view its diff please download" +
        " the objects and use an external diff tool."}/>
    }
    const leftSize = leftStat && leftStat.size_bytes;
    const rightSize = rightStat && rightStat.size_bytes;
    return <ContentDiff config={storageConfig} repoId={repoId} path={path} leftRef={left && leftRef} rightRef={right && rightRef}
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
    const supportedFileExtensions = supportedReadableFormats.map((fileType) => `.${fileType}`);
    return <div>
        <span><StatDiff left={left} right={right} diffType={diffType}/></span>
        <span><Alert variant="light"><InfoIcon/> {`lakeFS supports content diff for ${supportedFileExtensions.join(',')} file formats only`}</Alert></span>
    </div>;
}

const ContentDiff = ({config, repoId, path, leftRef, rightRef, leftSize, rightSize, diffType}) => {
    const {state} = useContext(AppContext);

    const left = leftRef && useAPI(async () => objects.get(repoId, leftRef, path, config.pre_sign_support_ui),
        [repoId, leftRef, path]);
    const right = rightRef && useAPI(async () => objects.get(repoId, rightRef, path, config.pre_sign_support_ui),
        [repoId, rightRef, path]);

    if ((left && left.loading) || (right && right.loading)) return <Loading/>;
    const err = (left && left.error) || (right && right.err);
    if (err) return <AlertError error={err}/>;

    return <div>
        <DiffSizeReport leftSize={leftSize} rightSize={rightSize} diffType={diffType}/>
        <ReactDiffViewer
            oldValue={left?.response}
            newValue={right?.response}
            splitView={false}
            useDarkTheme={state.settings.darkMode}
            compareMethod={DiffMethod.WORDS}
        />

    </div>;
}

function validateDiffInput(left, right, diffType) {
    switch (diffType) {
        case 'changed':
            if (!left && !right) return <AlertError error={"Invalid diff input"}/>;
            break;
        case 'added':
            if (!right) return <AlertError error={"Invalid diff input: right hand-side is missing"}/>;
            break;
        case 'removed':
            if (!left) return <AlertError error={"Invalid diff input: left hand-side is missing"}/>;
            break;
        case 'conflict':
            break;
        default:
            return <AlertError error={"Unknown diff type: " + diffType}/>;
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
            return <AlertError error={"Unknown diff type: " + diffType}/>;
    }


    return <div>
        <span className={label}>{label} </span>
        <span className={"diff-size"}>{humanSize(size)}</span>
        <span> in size</span>
    </div>;
}
