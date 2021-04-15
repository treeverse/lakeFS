import React from 'react';
import ReactDOM from 'react-dom';
import {BrowserRouter as Router, Switch, Route, Redirect} from 'react-router-dom';

// styles
import 'bootstrap/dist/css/bootstrap.css';
import './styles/globals.css';
import './styles/Home.module.css';

// pages
import Repositories from './pages/repositories'

ReactDOM.render(
  <React.StrictMode>
    <Router>
        <Switch>
            <Route exact path="/">
                <Redirect to="/repositories"/>
            </Route>

            <Route exact path="/repositories">
                <Repositories/>
            </Route>
        </Switch>
    </Router>
  </React.StrictMode>,
  document.getElementById('root')
);
