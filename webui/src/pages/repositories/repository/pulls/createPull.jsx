import React, {useCallback, useEffect, useState} from "react";
import {useOutletContext} from "react-router-dom";

import {ActionGroup, ActionsBar, AlertError, Loading} from "../../../../lib/components/controls";
import {useRefs} from "../../../../lib/hooks/repo";
import {RepoError} from "../error";
import {useRouter} from "../../../../lib/hooks/router";
import RefDropdown from "../../../../lib/components/repository/refDropdown";
import {ArrowLeftIcon, ArrowSwitchIcon} from "@primer/octicons-react";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import CompareBranches from "../../../../lib/components/repository/compareBranches";
import {RefTypeBranch} from "../../../../constants";
import Form from "react-bootstrap/Form";
import {pulls as pullsAPI} from "../../../../lib/api";

const CreatePullForm = ({repo, reference, compare}) => {
    const router = useRouter();
    let [loading, setLoading] = useState(false);
    let [error, setError] = useState(null);
    let [title, setTitle] = useState("");
    let [description, setDescription] = useState("");

    const onTitleInput = ({target: {value}}) => setTitle(value);
    const onDescriptionInput = ({target: {value}}) => setDescription(value);

    const submitForm = async () => {
        setLoading(true);
        setError(null);
        try {
            const createdPullId = await pullsAPI.create(repo.id, {
                title,
                description,
                source_branch: reference.id,
                destination_branch: compare.id
            });

            router.push({
                pathname: `/repositories/:repoId/pulls/:pullId`,
                params: {repoId: repo.id, pullId: createdPullId},
            });
        } catch (error) {
            console.error("Failed to create pull request", error);
            setError(error.message);
            setLoading(false);
        }
    }

    return <>
        <Form.Group className="mb-3">
            <Form.Control
                required
                disabled={loading}
                type="text"
                size="lg"
                placeholder="Add a title..."
                value={title}
                onChange={onTitleInput}
            />
        </Form.Group>
        <Form.Group className="mb-3">
            <Form.Control
                required
                disabled={loading}
                as="textarea"
                rows={8}
                placeholder="Describe your changes..."
                value={description}
                onChange={onDescriptionInput}
            />
        </Form.Group>
        {error &&
            <AlertError error={<div><p>Failed to create pull request:</p>{error}</div>}
                        onDismiss={() => setError(null)}/>}
        <Button variant="success"
                disabled={!title || !description || loading}
                onClick={submitForm}>
            {loading && <><span className="spinner-border spinner-border-sm text-light" role="status"/> {""}</>}
            Create Pull Request
        </Button>
    </>;
};

const CreatePull = () => {
    const router = useRouter()
    const {repo, loading, error, reference, compare} = useRefs();

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    const route = query => router.push({
        pathname: `/repositories/:repoId/pulls/create`,
        params: {repoId: repo.id},
        query
    });

    const onSelectRef = reference => route(compare ?
        {ref: reference.id, compare: compare.id} :
        {ref: reference.id}
    );
    const onSelectCompare = compare => route(reference ?
        {ref: reference.id, compare: compare.id} :
        {compare: compare.id}
    );

    return (
        <div>
            <CompareBranchesSelection
                repo={repo}
                reference={reference}
                onSelectRef={onSelectRef}
                compare={compare}
                onSelectCompare={onSelectCompare}
            />
            <h1 className="mt-3">Create Pull Request</h1>
            <div className="mt-4">
                <CreatePullForm repo={repo} reference={reference} compare={compare}/>
            </div>
            <hr className="mt-5 mb-4"/>
            <div className="w-75">
                <CompareBranches
                    repo={repo}
                    reference={{id: reference.id, type: RefTypeBranch}}
                    compareReference={{id: compare.id, type: RefTypeBranch}}
                />
            </div>
        </div>
    );
};

const CompareBranchesSelection = (
    {repo, reference, onSelectRef, compare, onSelectCompare}
) => {
    const router = useRouter();
    const handleSwitchRefs = useCallback((e) => {
        e.preventDefault();
        router.push({
            pathname: `/repositories/:repoId/pulls/create`,
            params: {repoId: repo.id},
            query: {reference: compare.id, compare: reference.id}
        });
    }, []);

    return <ActionsBar>
        <ActionGroup orientation="left">
            <RefDropdown
                prefix={'Base '}
                repo={repo}
                selected={(reference) ? reference : null}
                withCommits={false}
                withWorkspace={false}
                withTags={false}
                selectRef={onSelectRef}/>

            <ArrowLeftIcon className="me-2 mt-2" size="small" verticalAlign="middle"/>

            <RefDropdown
                prefix={'Compared to '}
                emptyText={'Compare with...'}
                repo={repo}
                selected={(compare) ? compare : null}
                withCommits={false}
                withWorkspace={false}
                withTags={false}
                selectRef={onSelectCompare}/>

            <OverlayTrigger placement="bottom" overlay={
                <Tooltip>Switch directions</Tooltip>
            }>
                    <span>
                        <Button variant={"link"}
                                onClick={handleSwitchRefs}>
                            <ArrowSwitchIcon className="me-2 mt-2" size="small" verticalAlign="middle"/>
                        </Button>
                    </span>
            </OverlayTrigger>
        </ActionGroup>
    </ActionsBar>;
};

const RepositoryCreatePullPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("pulls"), [setActivePage]);
    return <CreatePull/>;
}

export default RepositoryCreatePullPage;
