import React from "react";
import useUser from '../hooks/user'
import {auth, config} from "../api";
import {useRouter} from "../hooks/router";
import {Link} from "./nav";
import {useAPI} from "../hooks/api";
import {Navbar, Nav, NavDropdown} from "react-bootstrap";
import Container from "react-bootstrap/Container";
import {useLoginConfigContext} from "../hooks/conf";

const NavUserInfo = () => {
    const { user, loading, error } = useUser();
    const logoutUrl = useLoginConfigContext()?.logout_url || "/logout"
    const { response: versionResponse, loading: versionLoading, error: versionError } = useAPI(() => {
        return config.getLakeFSVersion()
    }, [])
    if (loading) return <Navbar.Text>Loading...</Navbar.Text>;
    if (!user || !!error) return (<></>);
    return (
        <NavDropdown title={user.friendly_name || user.id} className="navbar-username" align="end">
            <NavDropdown.Item
                onClick={()=> {
                    auth.clearCurrentUser();
                    window.location = logoutUrl;
                }}>
                Logout
            </NavDropdown.Item>
            <NavDropdown.Divider/>
            {!versionLoading && !versionError && <>
            <NavDropdown.Item disabled={true}>
                <small>lakeFS {versionResponse.version}</small>
            </NavDropdown.Item></>}
            <NavDropdown.Item disabled={true}>
                <small>Web UI __buildVersion</small>
            </NavDropdown.Item>
            {!versionLoading && !versionError && versionResponse.upgrade_recommended && <>
                <NavDropdown.Item href={versionResponse.upgrade_url}>
                    <small>upgrade recommended</small>
                </NavDropdown.Item></>}
        </NavDropdown>
    );
};

const TopNavLink = ({ href, children }) => {
    const router = useRouter();
    const isActive = (prefix) => router.route.indexOf(prefix) === 0;
    
    return (
        <Link component={Nav.Link} active={isActive(href)} href={href}>
            {children}
        </Link>
    );
};

const TopNav = ({logged = true}) => {
    if (!logged) {
        return (
            <Navbar variant="dark" bg="dark" expand="md">
            <Container fluid={true}>
                <Link component={Navbar.Brand} href="/">
                    <img src="/logo.png" alt="lakeFS" className="logo"/>
                </Link>
            </Container>
            </Navbar>
        );
    }
    return (
        <Navbar variant="dark" bg="dark" expand="md">
            <Container fluid={true}>
                <Link component={Navbar.Brand} href="/">
                    <img src="/logo.png" alt="lakeFS" className="logo"/>
                </Link>
                <Navbar.Toggle aria-controls="navbarScroll" />
                <Navbar.Collapse id="navbarScroll">

                    <Nav className="me-auto my-2 my-lg-0"
                         style={{ maxHeight: '100px' }}
                         navbarScroll>
                        <TopNavLink href="/repositories">Repositories</TopNavLink>
                        <TopNavLink href="/auth">Administration</TopNavLink>
                    </Nav>

                    <NavUserInfo/>
                </Navbar.Collapse>
            </Container>
        </Navbar>
    );
};

export default TopNav;
