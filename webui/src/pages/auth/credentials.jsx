import React, { useEffect } from 'react';
import { useOutletContext } from 'react-router-dom';
import { FeatureLockedEmptyState } from '../../lib/components/auth/enterpriseUpgrade';

const CredentialsPage = () => {
    const [setActiveTab] = useOutletContext();
    useEffect(() => setActiveTab('credentials'), [setActiveTab]);
    return <FeatureLockedEmptyState feature="credentials" />;
};

export default CredentialsPage;
