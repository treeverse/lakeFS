import React, {useContext, useEffect, useMemo, useRef, useState} from "react";
import { useOutletContext } from "react-router-dom";
import {
    TagIcon,
    LinkIcon,
    PackageIcon,
    TrashIcon
} from "@primer/octicons-react";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import ListGroup from "react-bootstrap/ListGroup";

import {tags} from "../../../lib/api";

import {
    ActionGroup,
    ActionsBar, ClipboardButton,
    AlertError, LinkButton,
    Loading, PrefixSearchWidget, RefreshButton
} from "../../../lib/components/controls";
import { useRefs } from "../../../lib/hooks/repo";
import { useAPIWithPagination } from "../../../lib/hooks/api";
import { Paginator } from "../../../lib/components/pagination";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import { Link } from "../../../lib/components/nav";
import { useRouter } from "../../../lib/hooks/router";
import {ConfirmationButton} from "../../../lib/components/modals";
import {RepoError} from "./error";
import {AppContext} from "../../../lib/hooks/appContext";


const TagWidget = ({ repo, tag, onDelete }) => {
    const {state} = useContext(AppContext);
    const buttonVariant = state.settings.darkMode ? "outline-light" : "outline-dark";

    return (
        <ListGroup.Item>
            <div className="clearfix">
                <div className="float-start">
                    <h6 className="mb-0">
                        <Link href={{
                            pathname: '/repositories/:repoId/objects',
                            params: { repoId: repo.id },
                            query: { ref: tag.id }
                        }}>
                            {tag.id}
                        </Link>
                    </h6>
                </div>

                <div className="float-end">
                    <ButtonGroup className="commit-actions">
                        <ConfirmationButton
                            variant="outline-danger"
                            msg={`Are you sure you wish to delete tag ${tag.id}?`}
                            tooltip="Delete tag"
                            onConfirm={() => {
                                tags.delete(repo.id, tag.id)
                                    .catch(err => alert(err))
                                    .then(() => onDelete(tag.id))
                            }}
                        >
                            <TrashIcon/>
                        </ConfirmationButton>
                    </ButtonGroup>
                    <ButtonGroup className="branch-actions ms-2">
                        <LinkButton
                            href={{
                                pathname: '/repositories/:repoId/commits/:commitId',
                                params: { repoId: repo.id, commitId: tag.commit_id },
                            }}
                            buttonVariant={buttonVariant}
                            tooltip="View referenced commit">
                            {tag.commit_id.substr(0, 12)}
                        </LinkButton>
                        <ClipboardButton variant={buttonVariant} text={tag.id} tooltip="Copy ID to clipboard" />
                        <ClipboardButton variant={buttonVariant} text={`lakefs://${repo.id}/${tag.id}`} tooltip="Copy URI to clipboard" icon={<LinkIcon />} />
                        <ClipboardButton variant={buttonVariant} text={`s3://${repo.id}/${tag.id}`} tooltip="Copy S3 URI to clipboard" icon={<PackageIcon />} />
                    </ButtonGroup>
                </div>
            </div>
        </ListGroup.Item>
    );
};


const CreateTagButton = ({ repo, variant = "success", onCreate = null, readOnly = false, children }) => {
    const [show, setShow] = useState(false);
    const [disabled, setDisabled] = useState(true);
    const [error, setError] = useState(null);
    const textRef = useRef(null);
    const defaultRef = useMemo(
        () => ({ id: repo.default_branch, type: "branch" }),
        [repo.default_branch]);
    const [selectedRef, setSelectedRef] = useState(defaultRef);

    const hide = () => {
        setShow(false);
    };

    const display = () => {
        setError(null);
        setDisabled(true);
        setShow(true);
    };

    const onSubmit = () => {
        setDisabled(true);
        setError(null);
        const tagId = textRef.current.value;
        const sourceRef = selectedRef.id;
        tags.create(repo.id, tagId, sourceRef)
            .then(response => {
                setError(null);
                setDisabled(false);
                setShow(false);
                if (onCreate !== null) onCreate(response);
            }, err => {
                setDisabled(false);
                setError(err);
            });
    };

    return (
        <>
            <Modal show={show} onHide={hide} enforceFocus={false}>
                <Modal.Header closeButton>
                    Create Tag
                </Modal.Header>
                <Modal.Body>

                    <Form onSubmit={(e) => {
                        onSubmit();
                        e.preventDefault();
                    }}>
                        <Form.Group controlId="name" className="float-start w-25">
                            <Form.Control type="text" placeholder="Tag Name" name="text" ref={textRef} onChange={() => setDisabled(!textRef.current || !textRef.current.value)}/>
                        </Form.Group>
                        <Form.Group controlId="source">
                            <span className="ms-2 me-2">@</span>
                            <RefDropdown
                                repo={repo}
                                prefix={'From '}
                                emptyText={'Select Source'}
                                selected={selectedRef}
                                selectRef={(refId) => {
                                    setSelectedRef(refId);
                                }}
                                withCommits={true}
                                withWorkspace={false} />
                        </Form.Group>
                    </Form>

                    {!!error && <AlertError error={error} />}

                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" onClick={onSubmit} disabled={disabled}>
                        Create
                    </Button>
                </Modal.Footer>
            </Modal>
            <Button variant={variant} disabled={readOnly} onClick={display}>{children}</Button>
        </>
    );
};


const EmptyTagsState = ({ repo, onCreateTag }) => {
    return (
        <div className="text-center py-5">
            <div className="mb-5">
                <div className="mb-4">
                    <h2 className="mb-3">
                        <TagIcon size={36} className="me-2"/>
                        No tags yet
                    </h2>
                    <p className="text-muted mb-4 fs-5">
                        Tags help you mark important points in your data&apos;s history, like releases, experiments, or versions. 
                        <br/>
                        They&apos;re perfect for <a 
                            href="https://docs.lakefs.io/latest/understand/use_cases/reproducibility/" 
                            target="_blank" 
                            rel="noopener noreferrer"
                            className="text-decoration-none"
                        >reproducibility</a> and making your data workflows more reliable. 
                    </p>
                        <p>
                        Learn more about <a 
                            href="https://docs.lakefs.io/latest/understand/model/#tags" 
                            target="_blank" 
                            rel="noopener noreferrer"
                            className="text-decoration-none"
                        >what tags are</a> and how to use them effectively.
                    </p>
                </div>
                
                <div className="mb-5">
                    <CreateTagButton 
                        repo={repo} 
                        readOnly={repo?.read_only} 
                        variant="success" 
                        onCreate={onCreateTag}
                        className="btn-lg px-4 py-2"
                    >
                        <TagIcon /> Create Your First Tag
                    </CreateTagButton>
                </div>
            </div>
        </div>
    );
};


const TagList = ({ repo, after, prefix, onPaginate }) => {
    const router = useRouter()
    const [refresh, setRefresh] = useState(true);
    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        return tags.list(repo.id, prefix, after);
    }, [repo.id, prefix, refresh, after]);

    const doRefresh = () => setRefresh(!refresh);
    let content;

    if (loading) content = <Loading />;
    else if (error) content = <AlertError error={error} />;
    else if (results && !!results.length) {
        content = (
            <>
                <Card>
                    <ListGroup variant="flush">
                        {results.map(tag => (
                            <TagWidget key={tag.id} repo={repo} tag={tag} onDelete={doRefresh} />
                        ))}
                    </ListGroup>
                </Card>
                <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after} />
                <div className={"mt-2"}>
                    A tag is an immutable pointer to a single commit. <a href="https://docs.lakefs.io/understand/model.html#tags" target="_blank" rel="noopener noreferrer">Learn more.</a>
                </div>
            </>
        );
    } else {
        content = <EmptyTagsState repo={repo} onCreateTag={doRefresh} />;
    }

    return (
        <>
            <div className="mb-5">
                <ActionsBar>
                    <ActionGroup orientation="right">
                        <PrefixSearchWidget
                            defaultValue={router.query.prefix}
                            text="Find Tag"
                            onFilter={prefix => router.push({
                                pathname: '/repositories/:repoId/tags',
                                params: {repoId: repo.id},
                                query: {prefix}
                            })}/>

                        <RefreshButton onClick={doRefresh} />

                        <CreateTagButton repo={repo} readOnly={repo?.read_only} variant="success" onCreate={doRefresh}>
                            <TagIcon /> Create Tag
                        </CreateTagButton>

                    </ActionGroup>
                </ActionsBar>
                {content}
            </div>
        </>
    );
};


const TagsContainer = () => {
    const router = useRouter()
    const { repo, loading, error } = useRefs();
    const { after } = router.query;
    const routerPfx = (router.query.prefix) ? router.query.prefix : "";

    if (loading) return <Loading />;
    if (error) return <RepoError error={error} />;

    return (
        <TagList
            repo={repo}
            after={(after) ? after : ""}
            prefix={routerPfx}
            onPaginate={after => {
                const query = { after };
                if (router.query.prefix) query.prefix = router.query.prefix;
                router.push({ pathname: '/repositories/:repoId/tags', params: { repoId: repo.id }, query });
            }} />
    );
};


const RepositoryTagsPage = () => {
  const [setActivePage] = useOutletContext();
  useEffect(() => setActivePage("tags"), [setActivePage]);
  return <TagsContainer />;
}

export default RepositoryTagsPage;
