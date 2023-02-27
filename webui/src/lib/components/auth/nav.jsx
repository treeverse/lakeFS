import React from "react";

import Nav from "react-bootstrap/Nav";
import Breadcrumb from "react-bootstrap/Breadcrumb";
import {BreadcrumbItem} from "react-bootstrap";
import {useLoginConfigContext} from "../../hooks/conf";

import {Link, NavItem} from "../nav";


export const UserNav = ({ userId, page = 'groups' }) => {
    return (
        <Nav justify variant="tabs">
            <Link component={NavItem} active={page === 'groups'} href={{pathname: '/auth/users/:userId/groups', params: {userId}}}>
                Group Memberships
            </Link>
            <Link component={NavItem} active={page === 'policies'} href={{pathname: '/auth/users/:userId/policies', params: {userId}}}>
                Directly Attached Policies
            </Link>
            <Link component={NavItem} active={page === 'effectivePolicies'} href={{pathname: '/auth/users/:userId/policies/effective', params: {userId}}}>
                Effective Attached Policies
            </Link>
            <Link component={NavItem} active={page === 'credentials'} href={{pathname: '/auth/users/:userId/credentials', params: {userId}}}>
                Access Credentials
            </Link>
        </Nav>
    );
};


export const GroupNav = ({ groupId, page = 'groups' }) => {
    const simplified = useLoginConfigContext()?.RBAC === 'simplified';
    return (
        <Nav justify variant="tabs">
            <Link component={NavItem} active={page === 'members'} href={{pathname: '/auth/groups/:groupId/members', params: {groupId}}}>
                Group Memberships
            </Link>
            {simplified ?
             <Link component={NavItem} active={page === 'acl'} href={{pathname: '/auth/groups/:groupId/acl', params: {groupId}}}>ACL</Link> :
             <Link component={NavItem} active={page === 'policies'} href={{pathname: '/auth/groups/:groupId/policies', params: {groupId}}}>
                Attached Policies
         </Link>}
        </Nav>
    );
};

export const UserHeader = ({ userEmail, userId, page }) => {
    return (
        <div className="mb-4">
            <Breadcrumb>
                <Link component={BreadcrumbItem} href='/auth/users'>
                    Users
                </Link>
                <Link component={BreadcrumbItem} href={{pathname: '/auth/users/:userId', params: {userId}}}>
                    {userEmail}
                </Link>
            </Breadcrumb>

            <UserNav userId={userId} page={page}/>
        </div>
    );
};

export const GroupHeader = ({ groupId, page }) => {
    return (
        <div className="mb-4">
            <Breadcrumb>
                <Link component={BreadcrumbItem} href='/auth/groups'>
                    Groups
                </Link>
                <Link component={BreadcrumbItem} href={{pathname: '/auth/groups/:groupId', params: {groupId}}}>
                    {groupId}
                </Link>
            </Breadcrumb>

            <GroupNav groupId={groupId} page={page}/>
        </div>
    );
};

export const PolicyHeader = ({ policyId }) => {
    return (
        <div className="mb-4">
            <Breadcrumb>
                <Link component={BreadcrumbItem} href='/auth/policies'>
                    Policies
                </Link>
                <Link component={BreadcrumbItem} href={{pathname: '/auth/policies/:policyId', params: {policyId}}}>
                    {policyId}
                </Link>
            </Breadcrumb>
        </div>
    );
};
