import React, { useMemo, useRef, useState } from "react";

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
    Error, LinkButton,
    Loading, RefreshButton
} from "../../../lib/components/controls";
import { RepositoryPageLayout } from "../../../lib/components/repository/layout";
import { RefContextProvider, useRefs } from "../../../lib/hooks/repo";
import { useAPIWithPagination } from "../../../lib/hooks/api";
import { Paginator } from "../../../lib/components/pagination";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import { Link } from "../../../lib/components/nav";
import { useRouter } from "../../../lib/hooks/router";
import {ConfirmationButton} from "../../../lib/components/modals";
import Alert from "react-bootstrap/Alert";
import {RepoError} from "./error";


const TagWidget = ({ repo, tag, onDelete }) => {

    const buttonVariant = "outline-dark";

    return (
        <ListGroup.Item>
            <div className="clearfix">
                <div className="float-left">
                    <h6>
                        <Link href={{
                            pathname: '/repositories/:repoId/objects',
                            params: { repoId: repo.id },
                            query: { ref: tag.id }
                        }}>
                            {tag.id}
                        </Link>
                    </h6>
                </div>

                <div className="float-right">
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
                    <ButtonGroup className="branch-actions ml-2">
                        <LinkButton
                            href={{
                                pathname: '/repositories/:repoId/commits/:commitId',
                                params: { repoId: repo.id, commitId: tag.commit_id },
                            }}
                            buttonVariant="outline-dark"
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


const CreateTagButton = ({ repo, variant = "success", onCreate = null, children }) => {
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
                        <Form.Group controlId="name" className="float-left w-25">
                            <Form.Control type="text" placeholder="Tag Name" name="text" ref={textRef} onChange={() => setDisabled(!textRef.current || !textRef.current.value)}/>
                        </Form.Group>
                        <Form.Group controlId="source">
                            <span className="ml-2 mr-2">@</span>
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

                    {!!error && <Error error={error} />}

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
            <Button variant={variant} onClick={display}>{children}</Button>
        </>
    );
};


const TagList = ({ repo, after, onPaginate }) => {

    const [refresh, setRefresh] = useState(true);
    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        return tags.list(repo.id, after);
    }, [repo.id, refresh, after]);

    const doRefresh = () => setRefresh(!refresh);

    let content;

    if (loading) content = <Loading />;
    else if (error) content = <Error error={error} />;
    else content = ( results && !!results.length  ?
        <>
            <Card>
                <ListGroup variant="flush">
                    {results.map(tag => (
                        <TagWidget key={tag.id} repo={repo} tag={tag} onDelete={doRefresh} />
                    ))}
                </ListGroup>
            </Card>
            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after} />
        </> : <Alert variant="info">There aren&apos;t any tags yet.</Alert>
    )

    return (
        <>
            <div className="mb-5">
                <ActionsBar>
                    <ActionGroup orientation="right">
                        <RefreshButton onClick={doRefresh} />

                        <CreateTagButton repo={repo} variant="success" onCreate={doRefresh}>
                            <TagIcon /> Create Tag
                        </CreateTagButton>

                    </ActionGroup>
                </ActionsBar>
                {content}
                <div className={"mt-2"}>
                    A tag is an immutable pointer to a single commit. <a href="https://docs.lakefs.io/understand/object-model.html#identifying-commits" target="_blank" rel="noopener noreferrer">Learn more.</a>
                </div>
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
    return (
        <RefContextProvider>
            <RepositoryPageLayout activePage={'tags'}>
                <TagsContainer />
            </RepositoryPageLayout>
        </RefContextProvider>
    )
}

export default RepositoryTagsPage;
