import React from 'react';
import { createRoot } from 'react-dom/client';

// styles
import 'bootstrap/dist/css/bootstrap.css';
import './styles/globals.css';

// main page
import {IndexPage} from './pages';

const container = document.getElementById('root');
if (!container) throw new Error("Failed to find root element!");

const root = createRoot(container);
root.render(<IndexPage />);
