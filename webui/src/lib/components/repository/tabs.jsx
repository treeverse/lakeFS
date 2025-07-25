import React from "react";

import Nav from "react-bootstrap/Nav";
import {GitCommitIcon, DatabaseIcon, GitBranchIcon, GitPullRequestIcon, GitCompareIcon, PlayIcon, GearIcon, TagIcon} from "@primer/octicons-react";

import {useRefs} from "../../hooks/repo";
import {Link, NavItem} from "../nav";
import {useRouter} from "../../hooks/router";

export const RepositoryNavTabs = ({ active }) => {
    const { reference } = useRefs();
    const router = useRouter();
    const { repoId } = router.params;

    const withRefContext = (url) => {
        const params = new URLSearchParams();
        if (reference) params.append('ref', reference.id);
        if (params.toString())
            return `${url}?${params.toString()}`;
        return url;
    };

    const withRefAndCompareContext = (url) => {
        const params = new URLSearchParams();
        if (reference) {
            params.append('ref', reference.id)
            params.append('compare', reference.id);
        }
        if (params.toString())
            return `${url}?${params.toString()}`;
        return url;
    };



    return (
        <Nav variant="tabs" >
            <Link active={active === 'objects'} href={withRefContext(`/repositories/${repoId}/objects`)} component={NavItem}>
                <DatabaseIcon/> Objects
            </Link>
            <Link active={active === 'commits'} href={withRefContext(`/repositories/${repoId}/commits`)} component={NavItem}>
                <GitCommitIcon/> Commits
            </Link>
            <Link active={active === 'branches'} href={`/repositories/${repoId}/branches`} component={NavItem}>
                <GitBranchIcon/> Branches
            </Link>
            <Link active={active === 'tags'} href={`/repositories/${repoId}/tags`} component={NavItem}>
                <TagIcon/> Tags
            </Link>
            <Link active={active === 'pulls'} href={`/repositories/${repoId}/pulls`} component={NavItem}>
                {/* TODO (gilo): the icon is very similar to the compare icon, consider changing it*/}
                <GitPullRequestIcon/> Pull Requests
            </Link>
            <Link active={active === 'compare'} href={withRefAndCompareContext(`/repositories/${repoId}/compare`)} component={NavItem}>
                <GitCompareIcon/> Compare
            </Link>
            <Link active={active === 'actions'} href={`/repositories/${repoId}/actions`} component={NavItem}>
                <PlayIcon/> Actions
            </Link>
            <Link active={active === 'settings'} href={`/repositories/${repoId}/settings`} component={NavItem}>
                <GearIcon/> Settings
            </Link>
        </Nav>
    );
};