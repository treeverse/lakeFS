import React from 'react';
import './App.css';
import 'bootstrap/dist/css/bootstrap.min.css';

import {connect} from 'react-redux';

import Navbar from 'react-bootstrap/Navbar';
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

import LoginForm from "./components/Login";
import RepoList from "./components/RepositoryList";
import NavDropdown from "react-bootstrap/NavDropdown";

import { logout } from './actions';

import {BrowserRouter as Router, Switch, Route, Redirect} from "react-router-dom";
import {ApiExplorer} from "./components/ApiExplorer";


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
            <Navbar bg="light" expand="lg">
                <Navbar.Brand href="#home">lakeFS Console</Navbar.Brand>
                <NavUserInfo/>
            </Navbar>
            <Container className={"main-app"}>
                <Router>
                    <Switch>
                        <Route path="/login">
                            <LoginForm/>
                        </Route>
                        <PrivateRoute path="/" user={user}>
                            <ApiExplorer/>
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
