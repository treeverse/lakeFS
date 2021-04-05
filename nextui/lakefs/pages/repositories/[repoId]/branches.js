import React, {useEffect, useMemo, useRef, useState} from "react";

import {useRouter} from "next/router";
import Link from "next/link";

import {BrowserIcon, GitBranchIcon, LinkIcon, PackageIcon, PlayIcon, SyncIcon} from "@primer/octicons-react";

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
    Loading, useDebouncedState
} from "../../../lib/components/controls";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {useRepo} from "../../../lib/hooks/repo";
import {useAPIWithPagination} from "../../../rest/hooks";
import {Paginator} from "../../../lib/components/pagination";
import moment from "moment";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import RefDropdown from "../../../lib/components/repository/refDropdown";


const BranchWidget = ({ repo, branch }) => {

    const buttonVariant = "primary"

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
                    </h6>
                </div>
                <div className="float-right">
                    <ButtonGroup className="branch-actions">

                    </ButtonGroup>

                    <div className="float-right ml-2">
                        <ButtonGroup className="commit-actions">

                        </ButtonGroup>
                    </div>
                </div>
            </div>
        </ListGroup.Item>
    )
}


const CreateBranchButton = ({ repo, variant = "success", children }) => {
    console.log('repo: ', repo)
    const [show, setShow] = useState(false)
    const [disabled, setDisabled] = useState(false)
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

                </Modal.Body>
                <Modal.Footer>
                    <Button variant="secondary" disabled={disabled} onClick={hide}>
                        Cancel
                    </Button>
                    <Button variant="success" onClick={hide}>
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

    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>

    return (
        <div className="mb-5">
            <ActionsBar>
                <ActionGroup orientation="left">

                </ActionGroup>

                <ActionGroup orientation="right">
                    <OverlayTrigger placement="bottom"
                                    overlay={<Tooltip id="refreshTooltipId">Refresh</Tooltip>}>
                        <Button variant="light" onClick={() =>  setRefresh(!refresh) }>
                            <SyncIcon/>
                        </Button>
                    </OverlayTrigger>


                    <CreateBranchButton repo={repo} variant="success">
                        <GitBranchIcon/> Create Branch
                    </CreateBranchButton>

                </ActionGroup>
            </ActionsBar>

            <Card>
                <ListGroup variant="flush">
                    {results.map(branch => (
                        <BranchWidget key={branch.id} repo={repo} branch={branch}/>
                    ))}
                </ListGroup>
            </Card>
            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
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