import React, {useCallback, useRef, useState} from "react";
import {refs as refsAPI} from "../../../lib/api";
import {RefTypeBranch} from "../../../constants";
import {ActionGroup, ActionsBar, AlertError, RefreshButton} from "../controls";
import {MetadataFields} from "./changes";
import {GitMergeIcon} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import {FormControl, FormHelperText, InputLabel, MenuItem, Select} from "@mui/material";
import CompareBranchesSelection from "./compareBranchesSelection";

const CompareBranchesActionsBar = (
    {repo, reference, compareReference, baseSelectURL, doRefresh, isEmptyDiff}
) => {
    return <ActionsBar>
        <ActionGroup orientation="left">
            <CompareBranchesSelection
                repo={repo}
                reference={reference}
                compareReference={compareReference}
                baseSelectURL={baseSelectURL}
                withCommits={true}
                />
        </ActionGroup>

        <ActionGroup orientation="right">

            <RefreshButton onClick={doRefresh}/>

            {(compareReference.type === RefTypeBranch && reference.type === RefTypeBranch) &&
                <MergeButton
                    repo={repo}
                    disabled={((compareReference.id === reference.id) || isEmptyDiff || repo?.read_only)}
                    source={compareReference.id}
                    dest={reference.id}
                    onDone={doRefresh}
                />
            }
        </ActionGroup>
    </ActionsBar>;
};

const MergeButton = ({repo, onDone, source, dest, disabled = false}) => {
    const textRef = useRef(null);
    const [metadataFields, setMetadataFields] = useState([])
    const initialMerge = {
        merging: false,
        show: false,
        err: null,
        strategy: "none",
    }
    const [mergeState, setMergeState] = useState(initialMerge);

    const onClickMerge = useCallback(() => {
            setMergeState({merging: mergeState.merging, err: mergeState.err, show: true, strategy: mergeState.strategy})
        }
    );

    const onStrategyChange = (event) => {
        setMergeState({
            merging: mergeState.merging,
            err: mergeState.err,
            show: mergeState.show,
            strategy: event.target.value
        });
    }
    const hide = () => {
        if (mergeState.merging) return;
        setMergeState(initialMerge);
        setMetadataFields([])
    }

    const onSubmit = async () => {
        const message = textRef.current.value;
        const metadata = {};
        metadataFields.forEach(pair => metadata[pair.key] = pair.value)

        let strategy = mergeState.strategy;
        if (strategy === "none") {
            strategy = "";
        }
        setMergeState({merging: true, show: mergeState.show, err: mergeState.err, strategy: mergeState.strategy})
        try {
            await refsAPI.merge(repo.id, source, dest, strategy, message, metadata);
            setMergeState({
                merging: mergeState.merging,
                show: mergeState.show,
                err: null,
                strategy: mergeState.strategy
            })
            onDone();
            hide();
        } catch (err) {
            setMergeState({merging: mergeState.merging, show: mergeState.show, err: err, strategy: mergeState.strategy})
        }
    }

    return (
        <>
            <Modal show={mergeState.show} onHide={hide} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Merge branch {source} into {dest}</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form className="mb-2">
                        <Form.Group controlId="message" className="mb-3">
                            <Form.Control type="text" placeholder="Commit Message (Optional)" ref={textRef}/>
                        </Form.Group>

                        <MetadataFields metadataFields={metadataFields} setMetadataFields={setMetadataFields}/>
                    </Form>
                    <FormControl sx={{m: 1, minWidth: 120}}>
                        <InputLabel id="demo-select-small" className="text-secondary">Strategy</InputLabel>
                        <Select
                            labelId="demo-select-small"
                            id="demo-simple-select-helper"
                            value={mergeState.strategy}
                            label="Strategy"
                            className="text-secondary"
                            onChange={onStrategyChange}
                        >
                            <MenuItem value={"none"}>Default</MenuItem>
                            <MenuItem value={"source-wins"}>source-wins</MenuItem>
                            <MenuItem value={"dest-wins"}>dest-wins</MenuItem>
                        </Select>
                    </FormControl>
                    <FormHelperText className="text-secondary">
                        In case of a merge conflict, this option will force the merge process
                        to automatically favor changes from <b>{dest}</b> (&rdquo;dest-wins&rdquo;) or
                        from <b>{source}</b> (&rdquo;source-wins&rdquo;). In case no selection is made,
                        the merge process will fail in case of a conflict.
                    </FormHelperText>
                    {(mergeState.err) ? (<AlertError error={mergeState.err}/>) : (<></>)}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={mergeState.merging} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={mergeState.merging} onClick={onSubmit}>
                        {(mergeState.merging) ? 'Merging...' : 'Merge'}
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant="success" disabled={disabled} onClick={() => onClickMerge()}>
                <GitMergeIcon/> {"Merge"}
            </Button>
        </>
    );
}

export default CompareBranchesActionsBar;
