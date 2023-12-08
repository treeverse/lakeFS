import React from 'react';
import ReactDOM from 'react-dom';

// styles
import 'bootstrap/dist/css/bootstrap.css';
import './styles/globals.css';

// main page
import {IndexPage} from './pages';

const container = document.getElementById('root');
if (!container) throw new Error("Failed to find root element!");
ReactDOM.render(<IndexPage />, container);
