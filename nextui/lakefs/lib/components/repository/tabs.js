
import Nav from "react-bootstrap/Nav";
import {FileDiffIcon, GitCommitIcon, DatabaseIcon, GitBranchIcon, GitCompareIcon, PlayIcon, GearIcon} from "@primer/octicons-react";

import {useRouter} from "next/router";
import {useRefs} from "../../hooks/repo";
import {NavItem} from "../nav";



export const RepositoryNavTabs = ({ active }) => {

    const { repo, reference, loading, error } = useRefs()

    const router = useRouter()

    const withRefContext = (url) => {
        const params = new URLSearchParams();
        if (!!reference) params.append('ref', reference.id)
        if (!!params.toString())
            return `${url}?${params.toString()}`
        return url
    }

    const withBranchContext = (url) => {
        const params = new URLSearchParams();
        if (!!reference && reference.type === 'branch') params.append('ref', reference.id)
        if (!!params.toString())
            return `${url}?${params.toString()}`
        return url
    }

    const withRefAndPathContext = (url) => {
        const { path } = router.query;
        const params = new URLSearchParams();
        if (!!reference) params.append('ref', reference.id)
        if (!!path) params.append('path', path)
        if (!!params.toString())
            return `${url}?${params.toString()}`
        return url
    }

    const repoId = (loading && !error) ? '' : repo.id

    return (
        <Nav justify variant="tabs" >
            <NavItem active={active === 'objects'} href={withRefAndPathContext(`/repositories/${repoId}/objects`)}>
                <DatabaseIcon/> Objects
            </NavItem>
            <NavItem active={active === 'changes'} href={withBranchContext(`/repositories/${repoId}/changes`)}>
                <FileDiffIcon/> Changes
            </NavItem>
            <NavItem active={active === 'commits'} href={withRefContext(`/repositories/${repoId}/commits`)}>
                <GitCommitIcon/> Commits
            </NavItem>
            <NavItem active={active === 'branches'} href={`/repositories/${repoId}/branches`}>
                <GitBranchIcon/> Branches
            </NavItem>
            <NavItem active={active === 'compare'} href={withRefContext(`/repositories/${repoId}/compare`)}>
                <GitCompareIcon/> Compare
            </NavItem>
            <NavItem active={active === 'actions'} href={`/repositories/${repoId}/actions`}>
                <PlayIcon/> Actions
            </NavItem>
            <NavItem active={active === 'settings'} href={`/repositories/${repoId}/settings`}>
                <GearIcon/> Settings
            </NavItem>
        </Nav>
    )
}