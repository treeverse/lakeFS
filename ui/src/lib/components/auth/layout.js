import Link from 'next/link';

import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Nav from "react-bootstrap/Nav";
import Card from "react-bootstrap/Card";

import Layout from "../layout";


export const AuthLayout = ({ children, activeTab }) => {
    return (
        <Layout>
            <Container fluid="xl">
                <Row className="mt-5">
                    <Col md={{span: 3}}>
                        <Card>
                            <Card.Header>
                                <Card.Title>Access Control</Card.Title>
                            </Card.Header>
                            <Card.Body>
                                <Nav variant="pills" className="flex-column">
                                    <Link passHref href="/auth/credentials">
                                        <Nav.Link active={activeTab === 'credentials'}>My Credentials</Nav.Link>
                                    </Link>
                                </Nav>

                                <hr/>

                                <Nav variant="pills" className="flex-column">
                                    <Link passHref href="/auth/users">
                                        <Nav.Link active={activeTab === 'users'}>Users</Nav.Link>
                                    </Link>
                                    <Link passHref href="/auth/groups">
                                        <Nav.Link active={activeTab === 'groups'}>Groups</Nav.Link>
                                    </Link>
                                    <Link passHref href="/auth/policies">
                                        <Nav.Link active={activeTab === 'policies'}>Policies</Nav.Link>
                                    </Link>
                                </Nav>
                            </Card.Body>
                        </Card>

                    </Col>
                    <Col md={{span: 9}}>
                        {children}
                    </Col>
                </Row>
            </Container>
        </Layout>
    );
};

