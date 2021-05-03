import React from "react";

import Nav from "react-bootstrap/Nav";
import {FileDiffIcon, GitCommitIcon, DatabaseIcon, GitBranchIcon, GitCompareIcon, PlayIcon, GearIcon} from "@primer/octicons-react";

import {useRefs} from "../../hooks/repo";
import {Link, NavItem} from "../nav";
import {useRouter} from "../../hooks/router";



export const RepositoryNavTabs = ({ active }) => {

    const { repo, reference, loading, error } = useRefs();

    const router = useRouter();

    const repoId = (loading && !error) ? '' : repo.id;

    const withRefContext = (url) => {
        const params = new URLSearchParams();
        if (!!reference) params.append('ref', reference.id);
        if (!!params.toString())
            return `${url}?${params.toString()}`;
        return url;
    };

    const withBranchContext = (url) => {
        const params = new URLSearchParams();
        if (!!reference && reference.type === 'branch') params.append('ref', reference.id);
        if (!!params.toString())
            return `${url}?${params.toString()}`;
        return url;
    };

    const withRefAndPathContext = (url) => {
        const { path } = router.query;
        const params = new URLSearchParams();
        if (!!reference) params.append('ref', reference.id);
        if (!!path) params.append('path', path);
        if (!!params.toString())
            return `${url}?${params.toString()}`;
        return url;
    };

    return (
        <Nav variant="tabs" >
            <Link active={active === 'objects'} href={withRefAndPathContext(`/repositories/${repoId}/objects`)} component={NavItem}>
                <DatabaseIcon/> Objects
            </Link>
            <Link active={active === 'changes'} href={withBranchContext(`/repositories/${repoId}/changes`)} component={NavItem}>
                <FileDiffIcon/> Changes
            </Link>
            <Link active={active === 'commits'} href={withRefContext(`/repositories/${repoId}/commits`)} component={NavItem}>
                <GitCommitIcon/> Commits
            </Link>
            <Link active={active === 'branches'} href={`/repositories/${repoId}/branches`} component={NavItem}>
                <GitBranchIcon/> Branches
            </Link>
            <Link active={active === 'compare'} href={withRefContext(`/repositories/${repoId}/compare`)} component={NavItem}>
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