import { combineReducers, createStore, applyMiddleware, compose } from 'redux';
import thunkMiddleware from 'redux-thunk'

import auth from './auth';
import repositories from './repositories';
import branches from './branches';

const reducer = combineReducers({ auth, repositories, branches });

const store = createStore(reducer, compose(applyMiddleware(thunkMiddleware)));

export default store;
