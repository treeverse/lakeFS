import { combineReducers, createStore, applyMiddleware, compose } from 'redux';
import thunkMiddleware from 'redux-thunk'

import auth from './auth';
import repositories from './repositories';
import branches from './branches';
import objects from './objects';
import loader from './loader';
import commits from './commits';
import refs from './refs';
import setup from './setup';
import config from './config';

const composeEnhancers = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose;

const reducer = combineReducers({ auth, repositories, branches, objects, commits, loader, refs, setup, config });

const store = createStore(reducer, composeEnhancers(applyMiddleware(thunkMiddleware)));

export default store;
