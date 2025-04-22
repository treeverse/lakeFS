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
import Alert from "react-bootstrap/Alert";
import {InfoIcon} from "@primer/octicons-react";

type AuthOutletContext = [(tab: string) => void];

const rbacDismissedKey = "lakefs:ui:acl:dismissRBACAlert";
export const AuthLayout = () => {
    const [showRBACAlert, setShowRBACAlert] = useState(!window.localStorage.getItem(rbacDismissedKey));
    const [activeTab, setActiveTab] = useState("credentials");  
    const {RBAC: rbac} = useLoginConfigContext();
    const [setIsLogged] = useLayoutOutletContext();
    useEffect(() => {
        setIsLogged(true);
    }, [setIsLogged]);
    return (
        <Container fluid="xl">
            <Row className="mt-5" >
                <div>
                    {rbac === 'simplified' && showRBACAlert &&
                    <Alert variant="info" title="rbac CTA" dismissible onClose={() => {
                        window.localStorage.setItem(rbacDismissedKey, "true");
                        setShowRBACAlert(false);
                    }}><InfoIcon/>{" "}Enhance Your Security with {" "}<Alert.Link href={"https://docs.lakefs.io/reference/security/rbac.html"}>Role-Based Access Control</Alert.Link>{" "}
                        – Available on <Alert.Link href={"https://lakefs.cloud/register"}>lakeFS Cloud</Alert.Link> and <Alert.Link href={"https://docs.lakefs.io/understand/enterprise/"}>lakeFS Enterprise</Alert.Link>!</Alert>
                    }
                </div>
                <Col md={{span: 3}}>
                        <Card>
                            <Card.Header>
                                <Card.Title>Access Control</Card.Title>
                            </Card.Header>
                            <Card.Body>
                                <Nav variant="pills" className="flex-column">
                                    <Link component={Nav.Link} href="/auth/credentials" active={activeTab === 'credentials'} disabled={rbac === "none"} >
                                        My Credentials
                                    </Link>
                                </Nav>

                                <hr/>

                                <Nav variant="pills" className="flex-column">
                                    <Link component={Nav.Link} href="/auth/users" active={activeTab === 'users'} disabled={rbac === "none"} >
                                        Users
                                    </Link>

                                    <Link component={Nav.Link} href="/auth/groups" active={activeTab === 'groups'} disabled={rbac === "none"} >
                                        Groups
                                    </Link>
                                    {rbac !== 'simplified' && rbac !== 'none' &&
                                        <Link component={Nav.Link} href="/auth/policies" active={activeTab === 'policies'} disabled={rbac === "none"} >
                                            Policies
                                        </Link>}
                                </Nav>
                            </Card.Body>
                        </Card>
                    </Col>
                <Col md={{span: 9}}>
                    {rbac === "none"?
                        (
                            <div>
                                <Alert variant="info" title="rbac CTA">
                                    <p><InfoIcon/>{" "}<b>Role-based access control not configured.</b></p>
                                    This feature is enabled on {" "}<Alert.Link href={"https://lakefs.cloud/register"}>lakeFS Cloud</Alert.Link>{" "}
                                    and <Alert.Link href={"https://docs.lakefs.io/understand/enterprise/"}>lakeFS Enterprise</Alert.Link>. {" "}
                                    <Alert.Link href={"https://lakefs.io/blog/why-moving-acls-out-of-core-lakefs/"}>Learn More</Alert.Link>
                                </Alert>
                            </div>
                        )
                        :
                        (
                            <Outlet context={[setActiveTab] satisfies AuthOutletContext}/>
                        )

                    }
                </Col>
            </Row>
        </Container>
    )
};

export function useAuthOutletContext() {
    return useOutletContext<AuthOutletContext>();
}
