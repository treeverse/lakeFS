import React from "react";
import useUser from '../hooks/user'
import {auth, config} from "../api";
import {useRouter} from "../hooks/router";
import {Link} from "./nav";
import {useAPI} from "../hooks/api";
import {Navbar, Nav, NavDropdown} from "react-bootstrap";
import Container from "react-bootstrap/Container";
import {useLoginConfigContext} from "../hooks/conf";
import {FeedPersonIcon} from "@primer/octicons-react";

const NavUserInfo = () => {
    const { user, loading, error } = useUser();
    const logoutUrl = useLoginConfigContext()?.logout_url || "/logout"
    const { response: versionResponse, loading: versionLoading, error: versionError } = useAPI(() => {
        return config.getLakeFSVersion()
    }, [])

    if (loading) return <Navbar.Text>Loading...</Navbar.Text>;
    if (!user || !!error) return (<></>);
    const notifyNewVersion = !versionLoading && !versionError && versionResponse.upgrade_recommended
    const renderNavBarTitle = () => {
        return (
        <>
            {notifyNewVersion && <> <div className="user-menu-notification-indicator"></div> </> }
            <FeedPersonIcon size={28} verticalAlign={"middle"}/> <span style={{marginLeft:6, fontSize:18}}>{user.friendly_name || user.id} </span>
        </>
        )
    }
    return (
        <NavDropdown title={renderNavBarTitle()} className="navbar-username" align="end">
            {notifyNewVersion && <>
            <NavDropdown.Item href={versionResponse.upgrade_url}>
                    <>
                    <div className="menu-item-notification-indicator"></div>
                    Newer LakeFS Version is Out!
                    </>
            </NavDropdown.Item><NavDropdown.Divider/></>}
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
