import React, {useCallback} from "react";
import {useRouter} from "../../hooks/router";
import RefDropdown from "./refDropdown";
import {ArrowLeftIcon, ArrowSwitchIcon} from "@primer/octicons-react";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";

const CompareBranchesSelection = (
    {repo, reference, compareReference, baseSelectURL, withCommits, withWorkspace, withTags}
) => {
    const router = useRouter();

    const handleSwitchRefs = useCallback((e) => {
        e.preventDefault();
        router.push({
            pathname: baseSelectURL,
            params: {repoId: repo.id},
            query: {ref: compareReference.id, compare: reference.id}
        });
    }, []);

    const route = query => router.push({
        pathname: baseSelectURL,
        params: {repoId: repo.id},
        query
    });

    const onSelectRef = reference => route(compareReference ?
        {ref: reference.id, compare: compareReference.id} :
        {ref: reference.id}
    );
    const onSelectCompare = compareReference => route(reference ?
        {ref: reference.id, compare: compareReference.id} :
        {compare: compareReference.id}
    );


    return <>
            <RefDropdown
                prefix={'Base '}
                repo={repo}
                selected={(reference) ? reference : null}
                withCommits={withCommits}
                withWorkspace={withWorkspace}
                withTags={withTags}
                selectRef={onSelectRef}/>

            <ArrowLeftIcon className="me-2 mt-2" size="small" verticalAlign="middle"/>

            <RefDropdown
                prefix={'Compared to '}
                emptyText={'Compare with...'}
                repo={repo}
                selected={(compareReference) ? compareReference : null}
                withCommits={withCommits}
                withWorkspace={withWorkspace}
                withTags={withTags}
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
        </>;
};

export default CompareBranchesSelection;
