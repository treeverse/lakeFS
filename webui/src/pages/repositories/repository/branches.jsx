import React, { useContext, useEffect, useMemo, useRef, useState } from "react";
import { useOutletContext } from "react-router-dom";
import {
    GitBranchIcon,
    GitCompareIcon,
    LinkIcon,
    PackageIcon,
    TrashIcon,
    CheckboxIcon,
    CheckIcon,
    DashIcon,
} from "@primer/octicons-react";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import ListGroup from "react-bootstrap/ListGroup";

import { branches } from "../../../lib/api";

import {
    ActionGroup,
    ActionsBar,
    ClipboardButton,
    AlertError,
    LinkButton,
    Loading,
    PrefixSearchWidget,
    RefreshButton,
    Checkbox,
    TooltipButton,
    ToggleSwitch,
} from "../../../lib/components/controls";
import { useRefs } from "../../../lib/hooks/repo";
import { useAPIWithPagination } from "../../../lib/hooks/api";
import { Paginator } from "../../../lib/components/pagination";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import Badge from "react-bootstrap/Badge";
import { ConfirmationButton } from "../../../lib/components/modals";
import Alert from "react-bootstrap/Alert";
import { Link } from "../../../lib/components/nav";
import { useRouter } from "../../../lib/hooks/router";
import { RepoError } from "./error";
import { Col, Row } from "react-bootstrap";
import { AppContext } from "../../../lib/hooks/appContext";

const ImportBranchName = "import-from-inventory";

const BranchWidget = ({ repo, branch, onDelete, selected = false, onSelect, onDeselect }) => {
    const { state } = useContext(AppContext);
    const router = useRouter();
    const buttonVariant = state.settings.darkMode ? "outline-light" : "outline-dark";
    const isDefault = repo.default_branch === branch.id;
    let deleteMsg = (
        <>
            Are you sure you wish to delete branch <strong>{branch.id}</strong> ?
        </>
    );
    if (branch.id === ImportBranchName) {
        deleteMsg = (
            <>
                <p>{deleteMsg}</p>
                <Alert variant="warning">
                    <strong>Warning</strong> this is a system branch used for importing data to
                    lakeFS
                </Alert>
            </>
        );
    }

    return (
        <ListGroup.Item style={selected ? { backgroundColor: "rgba(0, 123, 255, 0.1)" } : {}}>
            <Row className="d-flex align-items-center justify-content-between">
                <Col md="auto" className="d-flex align-items-center">
                    <Checkbox
                        key={`${branch.id}-${selected}`}
                        name={branch.id}
                        disabled={isDefault}
                        defaultChecked={selected}
                        onAdd={() => onSelect && onSelect(branch.id)}
                        onRemove={() => onDeselect && onDeselect(branch.id)}
                    />
                </Col>
                <Col
                    title={branch.id}
                    className="flex-grow-1 text-nowrap overflow-hidden text-truncate align-middle"
                >
                    <h6 className="mb-0">
                        <Link
                            href={{
                                pathname: "/repositories/:repoId/objects",
                                params: { repoId: repo.id },
                                query: { ref: branch.id },
                            }}
                        >
                            {branch.id}
                        </Link>

                        {isDefault && (
                            <>
                                {" "}
                                <Badge variant="info">Default</Badge>
                            </>
                        )}
                    </h6>
                </Col>

                <Col md="3" className="d-flex justify-content-end">
                    {!isDefault && (
                        <ButtonGroup className="commit-actions">
                            <ConfirmationButton
                                variant="outline-danger"
                                disabled={isDefault}
                                msg={deleteMsg}
                                tooltip="Delete branch"
                                onConfirm={() => {
                                    branches
                                        .delete(repo.id, branch.id)
                                        .catch((err) => alert(err))
                                        .then(() => onDelete(branch.id));
                                }}
                            >
                                <TrashIcon />
                            </ConfirmationButton>
                            <TooltipButton
                                variant={buttonVariant}
                                size="sm"
                                onClick={() => {
                                    router.push({
                                        pathname: "/repositories/:repoId/compare",
                                        params: { repoId: repo.id },
                                        query: { ref: repo.default_branch, compare: branch.id },
                                    });
                                }}
                                tooltip="Compare with default branch"
                            >
                                <GitCompareIcon />
                            </TooltipButton>
                        </ButtonGroup>
                    )}

                    <ButtonGroup className="branch-actions ms-2">
                        <LinkButton
                            href={{
                                pathname: "/repositories/:repoId/commits/:commitId",
                                params: { repoId: repo.id, commitId: branch.commit_id },
                            }}
                            buttonVariant={buttonVariant}
                            tooltip="View referenced commit"
                        >
                            {branch.commit_id.substr(0, 12)}
                        </LinkButton>
                        <ClipboardButton
                            variant={buttonVariant}
                            text={branch.id}
                            tooltip="Copy ID to clipboard"
                        />
                        <ClipboardButton
                            variant={buttonVariant}
                            text={`lakefs://${repo.id}/${branch.id}`}
                            tooltip="Copy URI to clipboard"
                            icon={<LinkIcon />}
                        />
                        <ClipboardButton
                            variant={buttonVariant}
                            text={`s3://${repo.id}/${branch.id}`}
                            tooltip="Copy S3 URI to clipboard"
                            icon={<PackageIcon />}
                        />
                    </ButtonGroup>
                </Col>
            </Row>
        </ListGroup.Item>
    );
};

const CreateBranchButton = ({
    repo,
    variant = "success",
    onCreate = null,
    readOnly = false,
    children,
}) => {
    const [show, setShow] = useState(false);
    const [disabled, setDisabled] = useState(false);
    const [error, setError] = useState(null);
    const textRef = useRef(null);
    const defaultBranch = useMemo(
        () => ({ id: repo.default_branch, type: "branch" }),
        [repo.default_branch],
    );
    const [selectedBranch, setSelectedBranch] = useState(defaultBranch);

    const hide = () => {
        if (disabled) return;
        setShow(false);
    };

    const display = () => {
        setShow(true);
    };

    const onSubmit = async () => {
        setDisabled(true);
        const branchId = textRef.current.value;
        const sourceRef = selectedBranch.id;

        try {
            await branches.create(repo.id, branchId, sourceRef);
            setError(false);
            setDisabled(false);
            setShow(false);
            onCreate();
        } catch (err) {
            setError(err);
            setDisabled(false);
        }
    };

    return (
        <>
            <Modal show={show} onHide={hide} enforceFocus={false}>
                <Modal.Header closeButton>Create Branch</Modal.Header>
                <Modal.Body>
                    <Form
                        onSubmit={(e) => {
                            onSubmit();
                            e.preventDefault();
                        }}
                    >
                        <Form.Group controlId="name" className="mb-3">
                            <Form.Control
                                type="text"
                                placeholder="Branch Name"
                                name="text"
                                ref={textRef}
                            />
                        </Form.Group>
                        <Form.Group controlId="source" className="mb-3">
                            <RefDropdown
                                repo={repo}
                                emptyText={"Select Source Branch"}
                                prefix={"From "}
                                selected={selectedBranch}
                                selectRef={(refId) => {
                                    setSelectedBranch(refId);
                                }}
                                withCommits={true}
                                withWorkspace={false}
                            />
                        </Form.Group>
                    </Form>

                    {!!error && <AlertError error={error} />}
                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={disabled} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" onClick={onSubmit} disabled={disabled}>
                        Create
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant={variant} disabled={readOnly} onClick={display}>
                {children}
            </Button>
        </>
    );
};

const navigateToBranches = (router, repoId, prefix, after, showHidden) => {
    const query = {};
    if (prefix) query.prefix = prefix;
    if (after) query.after = after;
    if (showHidden) query.show_hidden = "true";
    router.push({
        pathname: "/repositories/:repoId/branches",
        params: { repoId },
        query,
    });
};

const BranchList = ({ repo, prefix, after, showHidden = false, onPaginate }) => {
    const router = useRouter();
    const [refresh, setRefresh] = useState(true);
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [pendingFailedBranches, setPendingFailedBranches] = useState(null);
    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        return branches.list(repo.id, showHidden, prefix, after);
    }, [repo.id, refresh, prefix, after, showHidden]);

    const doRefresh = () => setRefresh(!refresh);

    useEffect(() => {
        setSelected([]);
        setPendingFailedBranches(null);
    }, [prefix, after, showHidden]);

    // Filter selection to only include failed branches that are still visible after refresh
    useEffect(() => {
        if (pendingFailedBranches && results.length > 0) {
            const visibleBranchIds = results.map((branch) => branch.id);
            const visibleFailedBranches = pendingFailedBranches.filter((id) =>
                visibleBranchIds.includes(id),
            );
            setSelected(visibleFailedBranches);
            setPendingFailedBranches(null);
        }
    }, [results, pendingFailedBranches]);

    const handleSelect = (branchId) => {
        setSelected((prev) => [...prev, branchId]);
    };

    const handleDeselect = (branchId) => {
        setSelected((prev) => prev.filter((id) => id !== branchId));
    };

    const handleSelectAll = () => {
        const selectableBranches = results
            .filter((branch) => branch.id !== repo.default_branch)
            .map((branch) => branch.id);
        setSelected(selectableBranches);
    };

    const handleSelectNone = () => {
        setSelected([]);
    };

    const handleToggleSelectAll = () => {
        const selectableBranches = results
            .filter((branch) => branch.id !== repo.default_branch)
            .map((branch) => branch.id);
        const selectableBranchesSet = new Set(selectableBranches);
        const visibleSelected = selected.filter((id) => selectableBranchesSet.has(id));
        const allSelected =
            selectableBranches.length > 0 && selectableBranches.length === visibleSelected.length;

        if (allSelected) {
            handleSelectNone();
        } else {
            handleSelectAll();
        }
    };

    const handleBulkDelete = async (hideModal) => {
        // Filter out default branch defensively
        const branchesToDelete = selected.filter((id) => id !== repo.default_branch);

        if (branchesToDelete.length === 0) {
            if (hideModal) hideModal();
            return;
        }

        // Clear any previous errors
        setDeleteError(null);

        // Delete branches sequentially
        const failedDeletions = [];
        for (const branchId of branchesToDelete) {
            try {
                await branches.delete(repo.id, branchId);
            } catch (error) {
                failedDeletions.push({
                    branchId: branchId,
                    error: error,
                });
            }
        }

        // Always refresh the view to show which branches were successfully deleted
        doRefresh();

        if (failedDeletions.length > 0) {
            const errorMessages = failedDeletions.map(
                (f) => `${f.branchId}: ${f.error?.message || "Unknown error"}`,
            );
            const errorDetails = {
                message: `Failed to delete ${failedDeletions.length} branch(es)`,
                failedBranches: failedDeletions.map((f) => f.branchId),
                errors: errorMessages,
            };
            setDeleteError(errorDetails);

            // Store failed branch IDs - useEffect will filter them against visible branches after refresh
            const failedBranchIds = failedDeletions.map((f) => f.branchId);
            setPendingFailedBranches(failedBranchIds);
        } else {
            // Clear selection on complete success
            setSelected([]);
            setPendingFailedBranches(null);
        }
        // Close the modal
        if (hideModal) hideModal();
    };

    let content;

    if (loading) content = <Loading />;
    else if (error) content = <AlertError error={error} />;
    else
        content = (
            <>
                {deleteError && (
                    <Alert
                        variant="danger"
                        dismissible
                        onClose={() => setDeleteError(null)}
                        className="mb-3"
                    >
                        <Alert.Heading>
                            {deleteError.message || "Failed to delete branches"}
                        </Alert.Heading>
                        {deleteError.errors && deleteError.errors.length > 0 && (
                            <div
                                style={{
                                    maxHeight: "200px",
                                    overflowY: "auto",
                                    marginTop: "0.5rem",
                                }}
                            >
                                <ul className="mb-0">
                                    {deleteError.errors.map((errorMsg, index) => (
                                        <li key={index}>{errorMsg}</li>
                                    ))}
                                </ul>
                            </div>
                        )}
                    </Alert>
                )}
                <Card>
                    <ListGroup variant="flush">
                        {results.map((branch) => (
                            <BranchWidget
                                key={branch.id}
                                repo={repo}
                                branch={branch}
                                onDelete={doRefresh}
                                selected={selected.includes(branch.id)}
                                onSelect={handleSelect}
                                onDeselect={handleDeselect}
                            />
                        ))}
                    </ListGroup>
                </Card>
                <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after} />
            </>
        );

    const selectableBranches = results
        .filter((branch) => branch.id !== repo.default_branch)
        .map((branch) => branch.id);
    const selectableBranchesSet = new Set(selectableBranches);

    // Filter selected to only include visible branches
    const visibleSelected = selected.filter((id) => selectableBranchesSet.has(id));
    const branchesToDelete = visibleSelected.filter((id) => id !== repo.default_branch);
    const deleteCount = branchesToDelete.length;

    const allSelected =
        selectableBranches.length > 0 && selectableBranches.length === visibleSelected.length;
    const someSelected = visibleSelected.length > 0 && !allSelected;
    const selectedCount = visibleSelected.length;

    // Determine which icon to show based on selection state
    let selectionIcon;
    if (allSelected) {
        selectionIcon = <CheckIcon />;
    } else if (someSelected) {
        selectionIcon = <DashIcon />;
    } else {
        selectionIcon = <CheckboxIcon />;
    }

    return (
        <div className="mb-5">
            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button
                        variant="light"
                        onClick={handleToggleSelectAll}
                        title={allSelected ? "Deselect all" : "Select all"}
                    >
                        {selectionIcon}
                    </Button>
                    {selectedCount > 0 && (
                        <span className="ms-2 align-self-center">{selectedCount} selected</span>
                    )}
                </ActionGroup>
                <ActionGroup orientation="right">
                    <PrefixSearchWidget
                        defaultValue={router.query.prefix}
                        text="Find branch"
                        onFilter={(prefix) => {
                            navigateToBranches(router, repo.id, prefix, null, showHidden);
                        }}
                    />

                    <RefreshButton onClick={doRefresh} />

                    <ConfirmationButton
                        onConfirm={handleBulkDelete}
                        disabled={deleteCount === 0}
                        variant="danger"
                        msg={
                            <>
                                Are you sure you&apos;d like to delete {deleteCount} branch
                                {deleteCount !== 1 ? "es" : ""}?
                                {branchesToDelete.length > 0 && branchesToDelete.length <= 10 && (
                                    <ul className="mt-2 mb-0">
                                        {branchesToDelete.map((branchId) => (
                                            <li key={branchId}>
                                                <strong>{branchId}</strong>
                                            </li>
                                        ))}
                                    </ul>
                                )}
                                {branchesToDelete.length > 10 && (
                                    <ul className="mt-2 mb-0">
                                        {branchesToDelete.slice(0, 10).map((branchId) => (
                                            <li key={branchId}>
                                                <strong>{branchId}</strong>
                                            </li>
                                        ))}
                                        <li>... and {branchesToDelete.length - 10} more</li>
                                    </ul>
                                )}
                                {branchesToDelete.some((id) => id === ImportBranchName) && (
                                    <Alert variant="warning" className="mt-2">
                                        <strong>Warning:</strong> One or more selected branches are
                                        system branches used for importing data to lakeFS
                                    </Alert>
                                )}
                            </>
                        }
                    >
                        Delete
                    </ConfirmationButton>

                    <CreateBranchButton
                        repo={repo}
                        readOnly={repo?.read_only}
                        variant="success"
                        onCreate={doRefresh}
                    >
                        <GitBranchIcon /> Create Branch
                    </CreateBranchButton>
                </ActionGroup>
            </ActionsBar>
            <div className={"ms-2 mb-3"}>
                <ToggleSwitch
                    label="Show hidden"
                    id="show-hidden-branches-toggle"
                    defaultChecked={showHidden}
                    onChange={(checked) => {
                        navigateToBranches(router, repo.id, router.query.prefix, null, checked);
                    }}
                />
            </div>
            {content}
            <div className={"mt-2"}>
                lakeFS uses a Git-like branching model.{" "}
                <a
                    href="https://docs.lakefs.io/understand/branching-model.html"
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    Learn more.
                </a>
            </div>
        </div>
    );
};

const BranchesContainer = () => {
    const router = useRouter();
    const { repo, loading, error } = useRefs();
    const { after } = router.query;
    const prefix = router.query.prefix || "";
    const showHidden = router.query.show_hidden === "true";

    if (loading) return <Loading />;
    if (error) return <RepoError error={error} />;

    return (
        <BranchList
            repo={repo}
            after={after ? after : ""}
            prefix={prefix}
            showHidden={showHidden}
            onPaginate={(after) => {
                navigateToBranches(router, repo.id, prefix, after, showHidden);
            }}
        />
    );
};

const RepositoryBranchesPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("branches"), [setActivePage]);
    return <BranchesContainer />;
};

export default RepositoryBranchesPage;
