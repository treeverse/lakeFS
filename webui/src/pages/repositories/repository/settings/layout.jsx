import React from "react";

import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Nav from "react-bootstrap/Nav";
import Card from "react-bootstrap/Card";
import {Link} from "../../../../lib/components/nav";
import {useRefs} from "../../../../lib/hooks/repo";
import {Loading} from "../../../../lib/components/controls";
import {RepoError} from "../error";


export const SettingsLayout = ({ children, activeTab }) => {
    const { repo, loading, error} = useRefs();
    if (loading) return <Loading/>;
    if (error) return <RepoError error={error}/>;
    const repoId = repo.id
    return (
            <Container fluid="xl">
                <Row className="mt-5">
                    <Col md={{span: 3}}>
                        <Card>
                            <Card.Header>
                                <Card.Title>Settings</Card.Title>
                            </Card.Header>
                            <Card.Body>
                                <Nav variant="pills" className="flex-column">
                                    <Link component={Nav.Link} href={{pathname: '/repositories/:repoId/settings/general', params: {repoId}}} active={activeTab === 'general'}>
                                        General
                                    </Link>
                                </Nav>
                                <Nav variant="pills" className="flex-column">
                                    <Link component={Nav.Link} href={{pathname: '/repositories/:repoId/settings/retention', params: {repoId }}} active={activeTab === 'retention'}>
                                        Garbage Collection
                                    </Link>
                                </Nav>
                                <Nav variant="pills" className="flex-column">
                                    <Link component={Nav.Link} href={{pathname: '/repositories/:repoId/settings/branches', params: {repoId }}} active={activeTab === 'branches'}>
                                        Branch Protection
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
    );
};

