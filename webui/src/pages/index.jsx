import React from 'react';

import {BrowserRouter as Router, Switch, Route, Redirect} from 'react-router-dom';

// pages
import Repositories from './repositories';
import Auth from './auth';
import Setup from './setup';

export const IndexPage = () => {
    return (
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
                <Route path="/setup">
                    <Setup/>
                </Route>
                <Redirect to="/repositories" />
            </Switch>
        </Router>
    );
};
