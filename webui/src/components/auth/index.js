import React from "react";
import {Row, Col, Nav, Card } from "react-bootstrap";

import {Switch, Route, useHistory, useLocation, Redirect} from "react-router-dom";

import {UserPage, UsersPage} from "./users";
import {GroupPage, GroupsPage} from "./groups";
import {PoliciesPage, PolicyPage} from "./policies";
import {CredentialsPage} from "./credentials";


export const AuthManagementPage = () => {
    return (
        <div className="mt-5 auth-page">
            <Row>
                <AuthNav/>
                <Switch>
                    <Route exact path={"/auth"}>
                        <Redirect to={"/auth/credentials"}/>
                    </Route>
                    <Route path={"/auth/users/:userId"}>
                        <UserPage/>
                    </Route>
                    <Route path={"/auth/users"}>
                        <UsersPage/>
                    </Route>
                    <Route path={"/auth/groups/:groupId"}>
                        <GroupPage/>
                    </Route>
                    <Route path={"/auth/groups"}>
                        <GroupsPage/>
                    </Route>
                    <Route path={"/auth/policies/:policyId"}>
                        <PolicyPage/>
                    </Route>
                    <Route path={"/auth/policies"}>
                        <PoliciesPage/>
                    </Route>
                    <Route path={"/auth/credentials"}>
                        <CredentialsPage/>
                    </Route>
                </Switch>
            </Row>
        </div>
    );
};


const AuthNav = () => {

    const location = useLocation();
    const history = useHistory();
    const isActive = prefix => location.pathname.indexOf(prefix) === 0;
    const onSelect = (href, e) => {
        e.preventDefault();
        history.push(href);
    };

    return (
        <Col lg={3}>
            <Card>
                <Card.Header>
                    Access Control
                </Card.Header>
                <Card.Body>

                    <Nav variant="pills" className="flex-column">
                        <Nav.Item>
                            <Nav.Link href={"/auth/credentials"} onSelect={onSelect} active={isActive("/auth/credentials")}>My Credentials</Nav.Link>
                        </Nav.Item>
                    </Nav>

                    <hr/>

                    <Nav variant="pills" className="flex-column">
                        <Nav.Item>
                            <Nav.Link href={"/auth/users"} onSelect={onSelect} active={isActive("/auth/users")}>Users</Nav.Link>
                        </Nav.Item>
                        <Nav.Item>
                            <Nav.Link href={"/auth/groups"} onSelect={onSelect} active={isActive("/auth/groups")}>Groups</Nav.Link>
                        </Nav.Item>
                        <Nav.Item>
                            <Nav.Link href={"/auth/policies"} onSelect={onSelect} active={isActive("/auth/policies")}>Policies</Nav.Link>
                        </Nav.Item>
                    </Nav>

                </Card.Body>
            </Card>
        </Col>
    );
}
