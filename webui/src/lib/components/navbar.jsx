import React from "react";
import Navbar from 'react-bootstrap/Navbar';
import Nav from 'react-bootstrap/Nav';
import NavDropdown from 'react-bootstrap/NavDropdown';
import useUser from '../hooks/user'
import {auth, config} from "../api";
import {useRouter} from "../hooks/router";
import {Link} from "./nav";
import {useAPI} from "../hooks/api";

const NavUserInfo = () => {
    const router = useRouter();
    const { user, loading, error } = useUser();
    const { response: versionResponse, loading: versionLoading, error: versionError } = useAPI(() => {
        return config.getLakeFSVersion()
    }, [])
    if (loading)  return <Navbar.Text>Loading...</Navbar.Text>;
    if (!user || !!error) return (<></>);
    return (
        <Navbar.Collapse className="justify-content-end">
            <NavDropdown title={user.friendly_name || user.id} className="navbar-username" alignRight>
                <NavDropdown.Header>
                    User: <code>{user.accessKeyId}</code>
                </NavDropdown.Header>

                <NavDropdown.Divider/>

                <NavDropdown.Item
                    href="/auth/credentials"
                    onSelect={()=> router.push('/auth/credentials')}>
                        Manage My Credentials
                </NavDropdown.Item>

                <NavDropdown.Item
                    onSelect={()=> {
                        auth.logout().then(() => {
                            router.push('/auth/login')
                        })
                    }}>
                    Logout
                </NavDropdown.Item>
                {!versionLoading && !versionError && <><NavDropdown.Divider/>
                <NavDropdown.Item disabled={true}>
                    <small>lakeFS {versionResponse.version}</small>
                </NavDropdown.Item></>}
                {!versionLoading && !versionError && versionResponse.upgrade_recommended && <>
                    <NavDropdown.Item href={versionResponse.upgrade_url}>
                        <small>upgrade recommended</small>
                    </NavDropdown.Item></>}
            </NavDropdown>
        </Navbar.Collapse>
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
                <Link component={Navbar.Brand} href="/">
                    <img src="/logo.png" alt="lakeFS" className="logo"/>
                </Link>
            </Navbar>
        );
    }
    return (
        <Navbar variant="dark" bg="dark" expand="md">
            <Link component={Navbar.Brand} href="/">
                <img src="/logo.png" alt="lakeFS" className="logo"/>
            </Link>

            <Nav className="mr-auto">
                <TopNavLink href="/repositories">Repositories</TopNavLink>
                <TopNavLink href="/auth">Administration</TopNavLink>
            </Nav>

            <NavUserInfo/>
        </Navbar>
    );
};

export default TopNav;
