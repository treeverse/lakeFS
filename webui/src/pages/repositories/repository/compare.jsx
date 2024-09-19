import React, {useEffect} from "react";
import {useOutletContext} from "react-router-dom";
import {Loading} from "../../../lib/components/controls";
import {useRefs} from "../../../lib/hooks/repo";
import {useRouter} from "../../../lib/hooks/router";
import {RepoError} from "./error";
import CompareBranches from "../../../lib/components/repository/compareBranches";

const CompareContainer = () => {
    const router = useRouter();
    const {loading, error, repo, reference, compare} = useRefs();

    const {prefix} = router.query;

    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;

    const route = query => router.push({
        pathname: `/repositories/:repoId/compare`,
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
        <CompareBranches
            repo={repo}
            reference={reference}
            compareReference={compare}
            showActionsBar={true}
            prefix={prefix}
            onSelectRef={onSelectRef}
            onSelectCompare={onSelectCompare}
        />
    );
};

const RepositoryComparePage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("compare"), [setActivePage]);
    return <CompareContainer/>;
};

export default RepositoryComparePage;
