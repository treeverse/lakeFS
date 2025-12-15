import React, { useCallback, useRef, useState } from "react";
import { refs as refsAPI, pulls as pullsAPI } from "../../../lib/api";
import { RefTypeBranch } from "../../../constants";
import { ActionGroup, ActionsBar, AlertError, RefreshButton } from "../controls";
import { MetadataFields } from "./metadata";
import { getMetadataIfValid, touchInvalidFields } from "./metadataHelpers";
import { GitMergeIcon, GitPullRequestIcon } from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import { FormControl, FormHelperText, InputLabel, MenuItem, Select } from "@mui/material";
import CompareBranchesSelection from "./compareBranchesSelection";
import { useRouter } from "../../../lib/hooks/router";

const CompareBranchesActionsBar = (
    { repo, reference, compareReference, baseSelectURL, doRefresh, isEmptyDiff }
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

            <RefreshButton onClick={doRefresh} />

            {(compareReference.type === RefTypeBranch && reference.type === RefTypeBranch) &&
                <>
                    <CreatePullRequestButton
                        repo={repo}
                        disabled={((compareReference.id === reference.id) || isEmptyDiff || repo?.read_only)}
                        source={compareReference.id}
                        dest={reference.id}
                    />
                    <MergeButton
                        repo={repo}
                        disabled={((compareReference.id === reference.id) || isEmptyDiff || repo?.read_only)}
                        source={compareReference.id}
                        dest={reference.id}
                        onDone={doRefresh}
                    />
                </>
            }
        </ActionGroup>
    </ActionsBar>;
};

const MergeButton = ({ repo, onDone, source, dest, disabled = false }) => {
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
        setMergeState({ merging: mergeState.merging, err: mergeState.err, show: true, strategy: mergeState.strategy })
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
        const metadata = getMetadataIfValid(metadataFields);
        if (!metadata) {
            setMetadataFields(touchInvalidFields(metadataFields));
            return;
        }

        const message = textRef.current.value;

        let strategy = mergeState.strategy;
        if (strategy === "none") {
            strategy = "";
        }
        setMergeState({ merging: true, show: mergeState.show, err: mergeState.err, strategy: mergeState.strategy })
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
            setMergeState({ merging: mergeState.merging, show: mergeState.show, err: err, strategy: mergeState.strategy })
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
                            <Form.Control
                                type="text"
                                placeholder="Commit Message (Optional)"
                                ref={textRef}
                                onKeyDown={(e) => {
                                    if (e.key === 'Enter' && !e.shiftKey) {
                                        e.preventDefault();
                                        onSubmit();
                                    }
                                }}
                            />
                        </Form.Group>

                        <MetadataFields metadataFields={metadataFields} setMetadataFields={setMetadataFields} />
                    </Form>
                    <FormControl sx={{ m: 1, minWidth: 120 }}>
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
                    {(mergeState.err) ? (<AlertError error={mergeState.err} />) : (<></>)}
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
                <GitMergeIcon /> {"Merge"}
            </Button>
        </>
    );
}

const CreatePullRequestButton = ({ repo, source, dest, disabled = false }) => {
    const router = useRouter();
    const descriptionRef = useRef(null);
    const [title, setTitle] = useState("");
    const [description, setDescription] = useState("");
    const initialPRState = {
        creating: false,
        show: false,
        err: null,
    }
    const [prState, setPRState] = useState(initialPRState);

    const onClickCreatePR = useCallback(() => {
        setPRState(prev => ({ ...prev, show: true }))
    }, []);

    const hide = () => {
        if (prState.creating) return;
        setPRState(initialPRState);
        setTitle("");
        setDescription("");
    }

    const onSubmit = async () => {
        const trimmedTitle = title.trim();
        const trimmedDescription = description.trim();

        if (!trimmedTitle) {
            return;
        }

        setPRState({ creating: true, show: prState.show, err: prState.err })
        try {
            const { id: createdPullId } = await pullsAPI.create(repo.id, {
                title: trimmedTitle,
                description: trimmedDescription,
                source_branch: source,
                destination_branch: dest
            });

            router.push({
                pathname: `/repositories/:repoId/pulls/:pullId`,
                params: { repoId: repo.id, pullId: createdPullId },
            });
        } catch (err) {
            setPRState({ creating: false, show: prState.show, err: err })
        }
    }

    return (
        <>
            <Modal show={prState.show} onHide={hide} size="lg">
                <Modal.Header closeButton>
                    <Modal.Title>Create Pull Request</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form className="mb-2">
                        <Form.Group controlId="title" className="mb-3">
                            <Form.Label>Title</Form.Label>
                            <Form.Control
                                type="text"
                                placeholder="Add a title..."
                                value={title}
                                onChange={(e) => setTitle(e.target.value)}
                                required
                                disabled={prState.creating}
                                onKeyDown={(e) => {
                                    if (e.key === 'Enter' && !e.shiftKey) {
                                        e.preventDefault();
                                        descriptionRef.current?.focus();
                                    }
                                }}
                            />
                        </Form.Group>
                        <Form.Group controlId="description" className="mb-3">
                            <Form.Label>Description</Form.Label>
                            <Form.Control
                                as="textarea"
                                rows={6}
                                placeholder="Describe your changes..."
                                value={description}
                                onChange={(e) => setDescription(e.target.value)}
                                ref={descriptionRef}
                                disabled={prState.creating}
                            />
                        </Form.Group>
                    </Form>
                    {(prState.err) ? (<AlertError error={prState.err} />) : (<></>)}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={prState.creating} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" disabled={prState.creating || !title.trim()} onClick={onSubmit}>
                        {(prState.creating) ? 'Creating...' : 'Create Pull Request'}
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant="primary" disabled={disabled} onClick={() => onClickCreatePR()}>
                <GitPullRequestIcon /> {"Create Pull Request"}
            </Button>
        </>
    );
}

export default CompareBranchesActionsBar;
