import React from 'react';

import {connect} from 'react-redux';
import Navbar from 'react-bootstrap/Navbar';
import Container from "react-bootstrap/Container";
import {BrowserRouter as Router, Switch, Route, Redirect} from "react-router-dom";

import LoginForm from "./components/Login";
import NavDropdown from "react-bootstrap/NavDropdown";
import { logout } from './actions/auth';
import {ApiExplorer} from "./components/ApiExplorer";

// css imports
import 'bootstrap/dist/css/bootstrap.min.css';
// import "bootswatch/dist/lux/bootstrap.min.css";
import './App.css';


let NavUserInfo = ({ user, logout }) => {
    if (!!user) {
        return (
            <Navbar.Collapse className="justify-content-end">
                <NavDropdown title={user.id} id="basic-nav-dropdown" alignRight>
                    <NavDropdown.Item onClick={(e) => { e.preventDefault(); logout(); }}>Logout</NavDropdown.Item>
                </NavDropdown>
            </Navbar.Collapse>
        );
    } else {
        return <span/>;
    }
}

NavUserInfo = connect(
    ({ auth }) => ({ user: auth.user }),
    ( dispatch ) => ({ logout: () => { dispatch(logout()); } }),
)(NavUserInfo);



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


const App = ({ user }) => {
    return (
        <div className="App">
            <Navbar variant="dark" bg="dark" expand="md">
                <Navbar.Brand href="/">lakeFS</Navbar.Brand>
                <NavUserInfo/>
            </Navbar>
            <Container className={"main-app"}>
                <Router>
                    <Switch>

                        <PrivateRoute path="/repositories" user={user}>
                            <ApiExplorer/>
                        </PrivateRoute>

                        <Route path="/login">
                            <LoginForm/>
                        </Route>

                        <PrivateRoute path="/" user={user}>
                            <Redirect to="/repositories"/>
                        </PrivateRoute>

                    </Switch>
                </Router>
            </Container>
        </div>
    );
};

export default connect(
    ({ auth }) => ({
        user: auth.user
    }), null)(App);
