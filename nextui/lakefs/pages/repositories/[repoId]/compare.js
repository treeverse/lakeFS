import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import {ActionGroup, ActionsBar, Error, Loading} from "../../../lib/components/controls";
import React, {useState} from "react";
import {useRepoRefAndCompare} from "../../../lib/hooks/repo";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {ArrowLeftIcon, GitMergeIcon, SyncIcon} from "@primer/octicons-react";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import {OverlayTrigger} from "react-bootstrap";
import {useAPIWithPagination} from "../../../rest/hooks";
import {branches, refs} from "../../../rest/api";
import Alert from "react-bootstrap/Alert";
import Card from "react-bootstrap/Card";
import Table from "react-bootstrap/Table";
import {ChangeEntryRow} from "../../../lib/components/repository/changes";
import {Paginator} from "../../../lib/components/pagination";
import {ConfirmationButton} from "../../../lib/components/modals";


const CompareContainer = ({ repo, reference, compareReference, after, onSelectRef, onSelectCompare, onPaginate }) => {
    if (compareReference === null) compareReference = reference

    const [internalRefresh, setInternalRefresh] = useState(true)
    const [mergeError, setMergeError] = useState(null)
    const [merging, setMerging] = useState(false)

    const refresh = () => setInternalRefresh(!internalRefresh)

    const { results, error, loading, nextPage } = useAPIWithPagination(async () => {
        if (compareReference.id !== reference.id)
            return refs.diff(repo.id, compareReference.id, reference.id, after)
        return {pagination: {has_more: false}, results: []} // nothing to compare here.
    }, [repo.id, reference.id, compareReference.id, internalRefresh, after])

    let content;
    if (loading) content = <Loading/>
    else if (!!error) content = <Error error={error}/>
    else if (!!mergeError) content = <Error error={mergeError}/>
    else if (compareReference.id === reference.id) content = (
        <Alert variant="warning">
            <Alert.Heading>There isn’t anything to compare.</Alert.Heading>
            You’ll need to use two different sources to get a valid comparison.
        </Alert>
    )
    else content = (
            <div className="tree-container">
                {(results.length === 0) ? <Alert variant="info">No changes</Alert> : (
                    <Card>
                        <Table borderless size="sm">
                            <tbody>
                            {results.map(entry => (
                                <ChangeEntryRow key={entry.path} entry={entry} showActions={false}/>
                            ))}
                            </tbody>
                        </Table>
                    </Card>
                )}

                <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
            </div>
    )

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <RefDropdown
                        prefix={'Base '}
                        repo={repo}
                        selected={(!!reference) ? reference : null}
                        withCommits={true}
                        withWorkspace={false}
                        selectRef={onSelectRef}/>

                <ArrowLeftIcon className="mr-2 mt-2"/>

                    <RefDropdown
                        prefix={'Compared to '}
                        emptyText={'Compare with...'}
                        repo={repo}
                        selected={(!!compareReference) ? compareReference : null}
                        withCommits={true}
                        withWorkspace={false}
                        selectRef={onSelectCompare}/>
                </ActionGroup>

                <ActionGroup orientation="right">
                    <OverlayTrigger placement="bottom"
                                    overlay={<Tooltip id="refreshCompareTooltipId">Refresh</Tooltip>}>
                        <Button variant="light" onClick={refresh}>
                            <SyncIcon/>
                        </Button>
                    </OverlayTrigger>

                    {(compareReference.type === 'branch' && reference.type === 'branch') &&
                    <ConfirmationButton
                        variant="success"
                        disabled={((compareReference.id === reference.id) || merging)}
                        msg={`Are you sure you'd like to merge '${compareReference.id}' into '${reference.id}'?`}
                        tooltip={`merge '${compareReference.id}' into '${reference.id}'`}
                        onConfirm={hide => {
                            setMerging(true)
                            hide()
                            refs.merge(repo.id, compareReference.id, reference.id)
                                .catch(err => setMergeError(err))
                                .then(() => {
                                    setMergeError(null)
                                    setMerging(false)
                                    refresh()
                                })
                        }}>
                        <GitMergeIcon/> {(merging) ? 'Merging...' : 'Merge'}
                    </ConfirmationButton>
                    }

                </ActionGroup>
            </ActionsBar>

            {content}
        </>
    )
}


const RefContainer = ({ repoId, refId, compareId, after, onSelectRef, onSelectCompare, onPaginate }) => {
    const {loading, error, response} = useRepoRefAndCompare(repoId, refId, compareId)
    if (loading) return <Loading/>
    if (!!error) return <Error error={error}/>
    const { repo, ref, compare } = response
    return (
        <CompareContainer
            repo={repo}
            after={after}
            reference={ref}
            onSelectRef={onSelectRef}
            compareReference={compare}
            onSelectCompare={onSelectCompare}
            onPaginate={onPaginate}
        />
    )
}

const RepositoryComparePage = () => {
    const router = useRouter()
    const { repoId, ref, compare, after } = router.query;

    const route = (query) => router.push({pathname: `/repositories/[repoId]/compare`, query})

    return (
        <RepositoryPageLayout repoId={repoId} activePage={'compare'}>
            {(!repoId) ?
                <Loading/> :
                <RefContainer
                    repoId={repoId}
                    refId={ref}
                    compareId={(!!compare) ? compare : ""}
                    after={(!!after) ? after : ""}
                    onSelectRef={ref => route({repoId, compare, ref: ref.id})}
                    onSelectCompare={compare => route({repoId, ref, compare: compare.id})}
                    onPaginate={after => route({repoId, ref, compare, after})}
                />
            }
        </RepositoryPageLayout>
    )
}

export default RepositoryComparePage;