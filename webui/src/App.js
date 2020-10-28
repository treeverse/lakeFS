import React, {useEffect} from 'react';

import {connect} from 'react-redux';
import Navbar from 'react-bootstrap/Navbar';
import Container from "react-bootstrap/Container";
import {BrowserRouter as Router, Switch, Route, Redirect, useLocation, useHistory, Link} from "react-router-dom";




import LoginForm from "./components/Login";
import SetupPage from "./components/SetupPage";
import NavDropdown from "react-bootstrap/NavDropdown";
import { logout, redirected } from './actions/auth';
import {getConfig} from "./actions/config";
import {IndexPage} from "./components/IndexPage";

// css imports
import 'bootswatch/dist/lumen/bootstrap.css';
import './App.css';
import Nav from "react-bootstrap/Nav";


const NavUserInfo = connect(
    ({ auth }) => ({ user: auth.user }),
    ( dispatch ) => ({ logout: () => { dispatch(logout()); } }),
)(({ user, logout }) => {
    if (!user) {
        return null;
    }
    return (
        <Navbar.Collapse className="justify-content-end">
            <NavDropdown title={user.id} className="navbar-username" alignRight>
                <NavDropdown.Header>
                    Access Key ID: <code>{user.accessKeyId}</code>
                </NavDropdown.Header>
                <NavDropdown.Divider/>
                <NavDropdown.Item as={Link} to={`/auth/credentials`}>Manage Credentials</NavDropdown.Item>
                <NavDropdown.Item onClick={(e) => { e.preventDefault(); logout(); }}>Logout</NavDropdown.Item>
            </NavDropdown>
        </Navbar.Collapse>
    );
})

const PrivateRoute = ({ children, user, ...rest }) => {
    return (
        <Route
            {...rest}
            render={({ location }) =>
                (!!user) ? (
                    children
                ) : (
                    <Redirect to={{pathname: "/login",  state: { from: location }}}/>
                )
            }
        />
    );
};


const TopBar = ({ user }) => {

    let loc = useLocation();
    let history = useHistory();

    const onNavigate = (href, e) => {
        e.preventDefault();
        history.push(href);
    };

    React.useEffect(() => {

    }, [loc])

    const isActive = (prefix) => {
        return loc.pathname.indexOf(prefix) === 0;
    };

    return (!!user) ? (
        <Navbar variant="dark" bg="dark" expand="md">
            <Navbar.Brand href="/">
                <img src="/logo.png" alt="lakeFS" className="logo"/>
            </Navbar.Brand>

            <Nav className="mr-auto">
                <Nav.Link href="/repositories" onSelect={onNavigate} active={isActive("/repositories")}>Repositories</Nav.Link>
                <Nav.Link href="/auth" onSelect={onNavigate} active={isActive("/auth")}>Administration</Nav.Link>
            </Nav>

            <NavUserInfo/>
        </Navbar>
    ): (
        <Navbar variant="dark" bg="dark" expand="md">
            <Navbar.Brand href="/">lakeFS</Navbar.Brand>
        </Navbar>
    )
}


const App = ({ user, redirectTo, redirected, getConfig }) => {

    useEffect(() => {
        getConfig();
      }, [getConfig]);

    return (
        <Router className="App">
            <TopBar user={user}/>
            <Container className={"main-app"}>
                <Switch>
                    <Route path="/login">
                        <LoginForm redirectTo={redirectTo} onRedirect={redirected}/>
                    </Route>
                    <Route path="/setup">
                        <SetupPage />
                    </Route>
                    <PrivateRoute path="/" user={user}>
                        <IndexPage/>
                    </PrivateRoute>
                </Switch>
            </Container>
        </Router>
    );
};

export default connect(
    ({ auth }) => ({
        user: auth.user,
        redirectTo: auth.redirectTo,
    }), ({ redirected, getConfig }))(App);
