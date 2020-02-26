import { combineReducers, createStore, applyMiddleware, compose } from 'redux';
import thunkMiddleware from 'redux-thunk'

import auth from './auth';
import repositories from './repositories';
import branches from './branches';
import objects from './objects';

const reducer = combineReducers({ auth, repositories, branches, objects });

const store = createStore(reducer, compose(applyMiddleware(thunkMiddleware)));

export default store;
