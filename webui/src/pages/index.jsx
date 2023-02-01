import React from 'react';

import {BrowserRouter as Router, Routes, Route, Navigate} from 'react-router-dom';
import {WithLoginContext} from "../lib/hooks/conf";


// pages
import Repositories from './repositories';
import Auth from './auth';
import Setup from './setup';

export const IndexPage = () => {
    return (
        <Router>
            <WithLoginContext>
                <Routes>
                    <Route path="/" element={<Navigate to="/repositories"/>} />
                    <Route path="/repositories/*" element={<Repositories/>} />
                    <Route path="/auth/*" element={<Auth/>} />
                    <Route path="/setup/*" element={<Setup/>} />
                    <Route path="*" element={<Navigate to="/repositories" replace />} />
                </Routes>
            </WithLoginContext>
        </Router>
    );
};
