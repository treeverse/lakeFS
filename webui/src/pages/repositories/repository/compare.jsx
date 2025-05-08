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

    return (
        <div className="mt-3">
            <h2>Branch Comparison</h2>
            <p className="text-muted">
                Compare files and data between branches. Select two branches to see their differences.
            </p>

            <CompareBranches
                repo={repo}
                reference={reference}
                compareReference={compare}
                showActionsBar={true}
                prefix={prefix}
                baseSelectURL={"/repositories/:repoId/compare"}
            />
        </div>
    );
};

const RepositoryComparePage = () => {
    const [setActivePage] = useOutletContext();
    useEffect(() => setActivePage("compare"), [setActivePage]);
    return <CompareContainer/>;
};

export default RepositoryComparePage;
