import Nav from "react-bootstrap/Nav";
import {NavItem} from "../nav";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import Link from "next/link";
import {BreadcrumbItem} from "react-bootstrap";

export const UserNav = ({ userId, page = 'groups' }) => {
    return (
        <Nav justify variant="tabs">
            <NavItem active={page === 'groups'} href={{pathname: '/auth/users/[userId]/groups', query: {userId}}}>
                Group Memberships
            </NavItem>
            <NavItem active={page === 'policies'} href={{pathname: '/auth/users/[userId]/policies', query: {userId}}}>
                Directly Attached Policies
            </NavItem>
            <NavItem active={page === 'effectivePolicies'} href={{pathname: '/auth/users/[userId]/effectivePolicies', query: {userId}}}>
                Effective Attached Policies
            </NavItem>
            <NavItem active={page === 'credentials'} href={{pathname: '/auth/users/[userId]/credentials', query: {userId}}}>
                Access Credentials
            </NavItem>
        </Nav>
    )
}

export const GroupNav = ({ groupId, page = 'groups' }) => {
    return (
        <Nav justify variant="tabs">
            <NavItem active={page === 'members'} href={{pathname: '/auth/groups/[groupId]/groups', query: {groupId}}}>
                Group Memberships
            </NavItem>
            <NavItem active={page === 'policies'} href={{pathname: '/auth/groups/[groupId]/policies', query: {groupId}}}>
                Attached Policies
            </NavItem>
        </Nav>
    )
}


export const UserHeader = ({ userId, page }) => {
    return (
        <div className="mb-4">
            <Breadcrumb>
                <Link href='/auth/users' passHref>
                    <BreadcrumbItem>Users</BreadcrumbItem>
                </Link>
                <Link href={{pathname: '/auth/users/[userId]', query: {userId}}} passHref>
                    <BreadcrumbItem>{userId}</BreadcrumbItem>
                </Link>
            </Breadcrumb>

            <UserNav userId={userId} page={page}/>
        </div>
    )
}

export const GroupHeader = ({ groupId, page }) => {
    return (
        <div className="mb-4">
            <Breadcrumb>
                <Link href='/auth/groups' passHref>
                    <BreadcrumbItem>Groups</BreadcrumbItem>
                </Link>
                <Link href={{pathname: '/auth/groups/[groupId]', query: {groupId}}} passHref>
                    <BreadcrumbItem>{groupId}</BreadcrumbItem>
                </Link>
            </Breadcrumb>

            <GroupNav groupId={groupId} page={page}/>
        </div>
    )
}

export const PolicyHeader = ({ policyId }) => {
    return (
        <div className="mb-4">
            <Breadcrumb>
                <Link href='/auth/policies' passHref>
                    <BreadcrumbItem>Policies</BreadcrumbItem>
                </Link>
                <Link href={{pathname: '/auth/policies/[policyId]', query: {policyId}}} passHref>
                    <BreadcrumbItem>{policyId}</BreadcrumbItem>
                </Link>
            </Breadcrumb>
        </div>
    )
}