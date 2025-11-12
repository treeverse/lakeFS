import React from "react";
import {useRouter} from "../hooks/router";
import {Link} from "./nav";
import DarkModeToggle from "./darkModeToggle";
import {Navbar, Nav, NavDropdown} from "react-bootstrap";
import Container from "react-bootstrap/Container";
import {useLoginConfigContext} from "../hooks/conf";
import {FeedPersonIcon} from "@primer/octicons-react";
import {useConfigContext} from "../hooks/configProvider";
import {auth} from "../api";
import {AUTH_STATUS, LAKEFS_POST_LOGIN_NEXT, useAuth} from "../auth/authContext";
import {ROUTES} from "../utils";

const NavUserInfo = () => {
    const { user, status } = useAuth();
    const userLoading = status === AUTH_STATUS.PENDING;
    const logoutUrl = useLoginConfigContext()?.logout_url || "/logout"
    const {config, error: versionError, loading: versionLoading} = useConfigContext();
    const versionConfig = config?.versionConfig || {};

    if (userLoading || versionLoading) return <Navbar.Text>Loading...</Navbar.Text>;
    if (!user) return (<></>);
    const notifyNewVersion = !versionLoading && !versionError && versionConfig.upgrade_recommended
    const NavBarTitle = () => {
        return (
        <>
            {notifyNewVersion && <> <div className="user-menu-notification-indicator"></div> </> }
            <FeedPersonIcon size={28} verticalAlign={"middle"}/> <span style={{marginLeft:6, fontSize:18}}>{user.friendly_name || user.id} </span>
        </>
        )
    }
    return (
        <NavDropdown title={<NavBarTitle />} className="navbar-username" align="end">
            {notifyNewVersion && <>
            <NavDropdown.Item href={versionConfig.upgrade_url}>
                    <>
                    <div className="menu-item-notification-indicator"></div>
                    New lakeFS version is available!
                    </>
            </NavDropdown.Item><NavDropdown.Divider/></>}
            <NavDropdown.Item
                onClick={() => {
                    auth.clearCurrentUser();
                    window.sessionStorage.removeItem(LAKEFS_POST_LOGIN_NEXT);
                    window.history.replaceState(null, "", `${ROUTES.LOGIN}?redirected=true`);
                    window.location.replace(logoutUrl);
                }}>
                Logout
            </NavDropdown.Item>
            <NavDropdown.Divider/>
            {!versionLoading && !versionError && <>
                <NavDropdown.Item disabled={true}>
                    {`${versionConfig.version_context} ${versionConfig.version}`}
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

const TopNav = () => {
    return (
        <Navbar variant="dark" bg="dark" expand="md" className="border-bottom">
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

                    <DarkModeToggle/>
                    <NavUserInfo/>
                </Navbar.Collapse>
            </Container>
        </Navbar>
    );
};

export default TopNav;
