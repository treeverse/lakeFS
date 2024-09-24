import React, {useEffect, useState} from "react";
import {useOutletContext} from "react-router-dom";

import {ActionGroup, ActionsBar, AlertError, Loading} from "../../../../lib/components/controls";
import {useRefs} from "../../../../lib/hooks/repo";
import {RepoError} from "../error";
import {useRouter} from "../../../../lib/hooks/router";
import Button from "react-bootstrap/Button";
import CompareBranches from "../../../../lib/components/repository/compareBranches";
import {RefTypeBranch} from "../../../../constants";
import Form from "react-bootstrap/Form";
import {pulls as pullsAPI} from "../../../../lib/api";
import CompareBranchesSelection from "../../../../lib/components/repository/compareBranchesSelection";

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
            const {id: createdPullId} = await pullsAPI.create(repo.id, {
                title,
                description,
                source_branch: compare.id,
                destination_branch: reference.id
            });

            router.push({
                pathname: `/repositories/:repoId/pulls/:pullId`,
                params: {repoId: repo.id, pullId: createdPullId},
            });
        } catch (error) {
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
        {error && <AlertError error={error} onDismiss={() => setError(null)}/>}
        <Button variant="success"
                disabled={!title || !description || loading}
                onClick={submitForm}>
            {loading && <><span className="spinner-border spinner-border-sm text-light" role="status"/> {""}</>}
            Create Pull Request
        </Button>
    </>;
};

const CreatePull = () => {
    const {repo, loading, error, reference, compare} = useRefs();

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    return (
        <div className="w-75">
            <ActionsBar>
                <ActionGroup orientation="left">
                    <CompareBranchesSelection
                        repo={repo}
                        reference={reference}
                        compareReference={compare}
                        baseSelectURL={"/repositories/:repoId/pulls/create"}
                    />
                </ActionGroup>
            </ActionsBar>
            <h1 className="mt-3">Create Pull Request</h1>
            <div className="mt-4">
                <CreatePullForm repo={repo} reference={reference} compare={compare}/>
            </div>
            <hr className="mt-5 mb-4"/>
            <CompareBranches
                repo={repo}
                reference={{id: reference.id, type: RefTypeBranch}}
                compareReference={{id: compare.id, type: RefTypeBranch}}
            />
        </div>
    );
};

const RepositoryCreatePullPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("pulls"), [setActivePage]);
    return <CreatePull/>;
}

export default RepositoryCreatePullPage;
