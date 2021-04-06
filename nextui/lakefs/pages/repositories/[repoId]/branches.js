import React, {useEffect, useMemo, useRef, useState} from "react";

import {useRouter} from "next/router";
import Link from "next/link";

import {
    BrowserIcon,
    GitBranchIcon,
    LinkIcon,
    PackageIcon,
    SyncIcon,
    TrashcanIcon,
    TrashIcon
} from "@primer/octicons-react";

import {branches} from "../../../rest/api";
import ButtonGroup from "react-bootstrap/ButtonGroup";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import Card from "react-bootstrap/Card";
import ListGroup from "react-bootstrap/ListGroup";

import {
    ActionGroup,
    ActionsBar, ClipboardButton,
    Error, LinkButton,
    Loading, useDebouncedState,
} from "../../../lib/components/controls";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {useRepo} from "../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../rest/hooks";
import {Paginator} from "../../../lib/components/pagination";
import moment from "moment";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import Badge from "react-bootstrap/Badge";
import {ConfirmationButton, ConfirmationModal} from "../../../lib/components/modals";
import Alert from "react-bootstrap/Alert";

const ImportBranchName = 'import-from-inventory';


const BranchWidget = ({ repo, branch, onDelete }) => {

    const buttonVariant = "primary"
    const isDefault = repo.default_branch === branch.id
    let deleteMsg = (
        <>
            Are you sure you wish to delete branch <strong>{branch.id}</strong> ?
        </>
    )
    if (branch.id === ImportBranchName) {
        deleteMsg = (
            <>
                <p>{deleteMsg}</p>
                <Alert variant="warning"><strong>Warning</strong> this is a system branch used for importing data to lakeFS</Alert>
            </>
        )
    }

    return (
        <ListGroup.Item>
            <div className="clearfix">
                <div className="float-left">
                    <h6>
                        <Link href={{
                            pathname: '/repositories/[repoId]/objects',
                            query: {repoId: repo.id, ref: branch.id}
                        }}>
                            <a>{branch.id}</a>
                        </Link>

                        {isDefault &&
                        <>
                            {' '}
                            <Badge variant="info">Default</Badge>
                        </>}
                    </h6>
                </div>


                <div className="float-right">
                    {!isDefault &&
                    <ButtonGroup className="commit-actions">
                        <ConfirmationButton
                            variant="outline-danger"
                            disabled={isDefault}
                            msg={deleteMsg}
                            tooltip="delete branch"
                            onConfirm={() => {
                                branches.delete(repo.id, branch.id)
                                    .catch(err => alert(err))
                                    .then(() => onDelete(branch.id))
                            }}
                        >
                            <TrashIcon/>
                        </ConfirmationButton>
                    </ButtonGroup>
                    }

                    <ButtonGroup className="branch-actions ml-2">
                        <LinkButton href={{
                            pathname: '/repositories/[repoId]/commits/[commitId]',
                            query: {repoId: repo.id, commitId: branch.commit_id}
                        }} buttonVariant="outline-primary" tooltip="View referenced commit">
                            {branch.commit_id.substr(0, 12)}
                        </LinkButton>
                        <ClipboardButton variant={buttonVariant} text={branch.id} tooltip="copy ID to clipboard"/>
                        <ClipboardButton variant={buttonVariant} text={`lakefs://${repo.id}@${branch.id}`} tooltip="copy URI to clipboard" icon={<LinkIcon/>}/>
                        <ClipboardButton variant={buttonVariant} text={`s3://${repo.id}/${branch.id}`} tooltip="copy S3 URI to clipboard" icon={<PackageIcon/>}/>
                    </ButtonGroup>
                </div>
            </div>
        </ListGroup.Item>
    )
}


const CreateBranchButton = ({ repo, variant = "success", onCreate = null, children }) => {
    const [show, setShow] = useState(false)
    const [disabled, setDisabled] = useState(false)
    const [error, setError] = useState(null)
    const textRef = useRef(null)
    const defaultBranch = useMemo(() => ({ id: repo.default_branch, type: "branch"}), [repo.id])
    const [selectedBranch, setSelectedBranch] = useState(defaultBranch)


    const hide = () => {
        if (disabled) return
        setShow(false)
    }

    const display = () => {
        setShow(true)
    }

    const onSubmit = () => {
        setDisabled(true)
        const branchId = textRef.current.value
        const sourceRef = selectedBranch.id
        branches.create(repo.id, branchId, sourceRef)
            .catch(err => {
                setError(err)
            })
            .then((response) => {
                setError(false)
                setDisabled(false)
                setShow(false)
                if (onCreate !== null) onCreate(response)
            })
    }

    return (
        <>
            <Modal show={show} onHide={hide}>
                <Modal.Header closeButton>
                    Create Branch
                </Modal.Header>
                <Modal.Body>

                    <Form onSubmit={(e) => {
                        onSubmit();
                        e.preventDefault();
                    }}>
                        <Form.Group controlId="name">
                            <Form.Control type="text" placeholder="Branch Name" name="text" ref={textRef}/>
                        </Form.Group>
                        <Form.Group controlId="source">
                            <RefDropdown
                                repo={repo}
                                emptyText={'Select Source Branch'}
                                prefix={'From '}
                                selected={selectedBranch}
                                selectRef={(refId) => {
                                    setSelectedBranch(refId);
                                }}
                                withCommits={true}
                                withWorkspace={false}/>
                        </Form.Group>
                    </Form>

                    {!!error && <Error error={error}/>}

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
    )
}

const BranchesContainer = ({ repo, prefix, after, onPaginate }) => {

    const [refresh, setRefresh] = useState(true)
    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        return branches.list(repo.id, prefix, after)
    }, [repo.id, refresh, prefix, after])

    const doRefresh = () =>  setRefresh(!refresh)

    let content;

    if (loading) content = <Loading/>
    else if (!!error) content = <Error error={error}/>
    else content = (
        <>
            <Card>
                <ListGroup variant="flush">
                    {results.map(branch => (
                        <BranchWidget key={branch.id} repo={repo} branch={branch} onDelete={doRefresh}/>
                    ))}
                </ListGroup>
            </Card>
            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
        </>
    )

    return (
        <div className="mb-5">
            <ActionsBar>
                <ActionGroup orientation="left">

                </ActionGroup>

                <ActionGroup orientation="right">
                    <OverlayTrigger placement="bottom"
                                    overlay={<Tooltip id="refreshTooltipId">Refresh</Tooltip>}>
                        <Button variant="light" onClick={doRefresh}>
                            <SyncIcon/>
                        </Button>
                    </OverlayTrigger>


                    <CreateBranchButton repo={repo} variant="success" onCreate={doRefresh}>
                        <GitBranchIcon/> Create Branch
                    </CreateBranchButton>

                </ActionGroup>
            </ActionsBar>
            {content}
        </div>
    )


}


const RefContainer = ({ repoId, prefix, after, onPaginate }) => {
    const {loading, error, response} = useRepo(repoId)
    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>
    const  repo = response

    return (
        <BranchesContainer repo={repo} after={after} prefix={prefix} onPaginate={onPaginate}/>
    )
}



const RepositoryBranchesPage = () => {
    const router = useRouter()
    const { repoId, after } = router.query;

    const routerPfx = (!!router.query.prefix) ? router.query.prefix : ""
    const [prefix, setPrefix] = useDebouncedState(
        routerPfx,
        (prefix) => router.push({pathname: `/repositories/[repoId]/branches`, query: {repoId, prefix}})
    )

    return (
        <RepositoryPageLayout repoId={repoId} activePage={'branches'}>
            {(!repoId) ?
                <Loading/> :
                <RefContainer
                    repoId={repoId}
                    after={(!!after) ? after : ""}
                    prefix={routerPfx}
                    onPaginate={after => {
                        const query = {repoId, after}
                        if (!!router.query.prefix) query.prefix = router.query.prefix
                        router.push({pathname: `/repositories/[repoId]/branches`, query})
                    }}
                />
            }
        </RepositoryPageLayout>
    )
}

export default RepositoryBranchesPage;