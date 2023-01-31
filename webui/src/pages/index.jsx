import React from 'react';

import {BrowserRouter as Router, Routes, Route, Navigate} from 'react-router-dom';
import {AuthModeContext, simplified} from './context.jsx';

// pages
import Repositories from './repositories';
import Auth from './auth';
import Setup from './setup';

export const IndexPage = () => {
    return (
        <Router>
            <AuthModeContext.Provider value={simplified}>
                <Routes>
                    <Route path="/" element={<Navigate to="/repositories"/>} />
                    <Route path="/repositories/*" element={<Repositories/>} />
                    <Route path="/auth/*" element={<Auth/>} />
                    <Route path="/setup/*" element={<Setup/>} />
                    <Route path="*" element={<Navigate to="/repositories" replace />} />
                </Routes>
            </AuthModeContext.Provider>
        </Router>
    );
};
