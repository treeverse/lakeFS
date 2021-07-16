import React, { useMemo, useRef, useState } from "react";

import {
    TagIcon,
    LinkIcon,
    PackageIcon,
    SearchIcon
} from "@primer/octicons-react";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import ListGroup from "react-bootstrap/ListGroup";

import { tags } from "../../../lib/api";

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


const TagWidget = ({ repo, tag }) => {

    const buttonVariant = "outline-dark";

    return (
        <ListGroup.Item>
            <div className="clearfix">
                <div className="float-left">
                    <h6>
                        <Link href={{
                            pathname: '/repositories/:repoId/objects',
                            params: { repoId: repo.id },
                            query: { ref: tag.commit_id }
                        }}>
                            {tag.id}
                        </Link>
                    </h6>
                </div>

                <div className="float-right">

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
    const [disabled, setDisabled] = useState(false);
    const [error, setError] = useState(null);
    const textRef = useRef(null);
    const defaultBranch = useMemo(
        () => ({ id: repo.default_branch, type: "branch" }),
        [repo.default_branch]);
    const [selectedBranch, setSelectedBranch] = useState(defaultBranch);

    const hide = () => {
        if (disabled) return;
        setShow(false);
    };

    const display = () => {
        setShow(true);
    };

    const onSubmit = () => {
        setDisabled(true);
        const tagId = textRef.current.value;
        const sourceRef = selectedBranch.id;
        tags.create(repo.id, tagId, sourceRef)
            .catch(err => {
                setError(err);
            })
            .then((response) => {
                setError(false);
                setDisabled(false);
                setShow(false);
                if (onCreate !== null) onCreate(response);
            });
    };

    return (
        <>
            <Modal show={show} onHide={hide}>
                <Modal.Header closeButton>
                    Create Tag
                </Modal.Header>
                <Modal.Body>

                    <Form onSubmit={(e) => {
                        onSubmit();
                        e.preventDefault();
                    }}>
                        <Form.Group controlId="name" className="float-left w-25">
                            <Form.Control type="text" placeholder="Tag Version" name="text" ref={textRef} />
                        </Form.Group>
                        <Form.Group controlId="source">
                            <span className="ml-2 mr-2">@</span>
                            <RefDropdown
                                repo={repo}
                                emptyText={'Select Branch'}
                                selected={selectedBranch}
                                selectRef={(refId) => {
                                    setSelectedBranch(refId);
                                }}
                                withCommits={true}
                                withWorkspace={false} />
                        </Form.Group>
                    </Form>

                    {!!error && <Error error={error} />}

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
    else if (!!error) content = <Error error={error} />;
    else content = (
        <>
            <Card>
                <ListGroup variant="flush">
                    {results.map(tag => (
                        <TagWidget key={tag.id} repo={repo} tag={tag} onDelete={doRefresh} />
                    ))}
                </ListGroup>
            </Card>
            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after} />
        </>
    );

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
            </div>
        </>
    );
};


const TagsContainer = () => {
    const router = useRouter()
    const { repo, loading, error } = useRefs();
    const { after } = router.query;
    const routerPfx = (!!router.query.prefix) ? router.query.prefix : "";

    if (loading) return <Loading />;
    if (!!error) return <Error error={error} />;

    return (
        <TagList
            repo={repo}
            after={(!!after) ? after : ""}
            prefix={routerPfx}
            onPaginate={after => {
                const query = { after };
                if (!!router.query.prefix) query.prefix = router.query.prefix;
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