import React, {useEffect} from "react";
import {useOutletContext} from "react-router-dom";

import {Loading} from "../../../../lib/components/controls";
import {useRefs} from "../../../../lib/hooks/repo";
import {RepoError} from "../error";

const CreatePull = () => {
    const {repo, loading, error} = useRefs();

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    return (
        <div>
            <h1>Create Pull Request (in repo {repo.id})</h1>
            <div>TBD</div>
        </div>
    );
};


const RepositoryCreatePullPage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("pulls"), [setActivePage]);
    return <CreatePull/>;
}

export default RepositoryCreatePullPage;
