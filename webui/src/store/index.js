import { combineReducers, createStore, applyMiddleware, compose } from 'redux';
import thunkMiddleware from 'redux-thunk'

import auth from './auth';
import repositories from './repositories';

const reducer = combineReducers({ auth, repositories });

const store = createStore(reducer, compose(applyMiddleware(thunkMiddleware)));

export default store;
