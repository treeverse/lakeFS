import React, {useEffect, useState} from "react";
import {Outlet, useOutletContext} from "react-router-dom";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Nav from "react-bootstrap/Nav";
import Card from "react-bootstrap/Card";

import {Link} from "../nav";
import {useLoginConfigContext} from "../../hooks/conf";
import {useLayoutOutletContext} from "../layout";

type AuthOutletContext = [(tab: string) => void];


export const AuthLayout = () => {
    const [activeTab, setActiveTab] = useState("credentials");
    const {RBAC: rbac} = useLoginConfigContext();
    const [setIsLogged] = useLayoutOutletContext();
    useEffect(() => {
        setIsLogged(true);
    }, [setIsLogged]);
    return (
        <Container fluid="xl">
            <Row className="mt-5">
                <Col md={{span: 3}}>
                    <Card>
                        <Card.Header>
                            <Card.Title>Access Control</Card.Title>
                        </Card.Header>
                        <Card.Body>
                            <Nav variant="pills" className="flex-column">
                                <Link component={Nav.Link} href="/auth/credentials" active={activeTab === 'credentials'}>
                                    My Credentials
                                </Link>
                            </Nav>

                            <hr/>

                            <Nav variant="pills" className="flex-column">
                                <Link component={Nav.Link} href="/auth/users" active={activeTab === 'users'}>
                                    Users
                                </Link>

                                <Link component={Nav.Link} href="/auth/groups" active={activeTab === 'groups'}>
                                    Groups
                                </Link>
    {rbac !== 'simplified' &&
        <Link component={Nav.Link} href="/auth/policies" active={activeTab === 'policies'}>
        Policies
        </Link>}
                            </Nav>
                        </Card.Body>
                    </Card>

                </Col>
                <Col md={{span: 9}}>
                    <Outlet context={[setActiveTab] satisfies AuthOutletContext} />
                </Col>
            </Row>
        </Container>
    );
};

export function useAuthOutletContext() {
    return useOutletContext<AuthOutletContext>();
}