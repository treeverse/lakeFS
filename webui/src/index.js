import React from 'react';
import ReactDOM from 'react-dom';
import {BrowserRouter as Router, Switch, Route, Redirect} from 'react-router-dom';

// styles
import 'bootstrap/dist/css/bootstrap.css';
import './styles/globals.css';

// pages
import Repositories from './pages/repositories'
import Auth from "./pages/auth";

ReactDOM.render(
    <Router>
        <Switch>
            <Route exact path="/">
                <Redirect to="/repositories"/>
            </Route>
            <Route path="/repositories">
                <Repositories/>
            </Route>
            <Route path="/auth">
                <Auth/>
            </Route>
        </Switch>
    </Router>,
  document.getElementById('root')
);
