
import Nav from "react-bootstrap/Nav";
import Alert from "react-bootstrap/Alert";
import {FileDiffIcon, GitCommitIcon, DatabaseIcon, GitBranchIcon, GitCompareIcon, PlayIcon, GearIcon} from "@primer/octicons-react";

import Link from 'next/link';
import {useRouter} from "next/router";


const NavItem = ({ href, active, children }) => {
    return (
        <Nav.Item>
            <Link href={href}>
                <Nav.Link href={href} active={active}>
                    <>{children}</>
                </Nav.Link>
            </Link>
        </Nav.Item>
    )
}

export const RepositoryNavTabs = ({ repoId, active }) => {

    const router = useRouter()

    const withRefContext = (url) => {
        const { ref } = router.query;
        const params = new URLSearchParams();
        if (!!ref)
            params.append('ref', ref)
        if (!!params.toString())
            return `${url}?${params.toString()}`
        return url
    }

    const withRefAndPathContext = (url) => {
        const { ref, path } = router.query;
        const params = new URLSearchParams();
        if (!!ref)
            params.append('ref', ref)
        if (!!path)
            params.append('path', path)
        if (!!params.toString())
            return `${url}?${params.toString()}`
        return url
    }

    return (
        <Nav justify variant="tabs" >
            <NavItem active={active === 'objects'} href={withRefAndPathContext(`/repositories/${repoId}/objects`)}>
                <DatabaseIcon/> Objects
            </NavItem>
            <NavItem active={active === 'changes'} href={withRefAndPathContext(`/repositories/${repoId}/changes`)}>
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