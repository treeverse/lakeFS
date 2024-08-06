import React, {useEffect, useState} from "react";
import {Outlet, useOutletContext} from "react-router-dom";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import Nav from "react-bootstrap/Nav";
import Card from "react-bootstrap/Card";

import {auth} from "../../api";
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
    const [displayACLDeprecation, setDisplayACLDeprecation] =  useState(false);
    useEffect(() => {
        const listUsers = async () => {
            return await auth.listUsers("", "", 2);
        }
        listUsers().then(r => setDisplayACLDeprecation(r.results.length > 1));
    },[])
    return (
        <Container fluid="xl">
            <Row className="mt-5">
                <div>
                    { displayACLDeprecation &&
                    <Alert variant="warning" title="ACL Deprecation"><InfoIcon/>{" "}<b>ACLs are moving out of core lakeFS!</b>{"  "}See the <Alert.Link href={"https://lakefs.io/blog/why-moving-acls-out-of-core-lakefs/"}>announcement</Alert.Link>{" "}
                        to learn why and how to continue using your existing lakeFS installation in future versions.</Alert>
                    }
                    {rbac === 'simplified' &&  showRBACAlert &&
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