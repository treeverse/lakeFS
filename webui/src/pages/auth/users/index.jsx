import React, { useEffect } from 'react';
import { useOutletContext } from 'react-router-dom';
import { FeatureLockedEmptyState } from '../../../lib/components/auth/enterpriseUpgrade';

const UsersIndexPage = () => {
    const [setActiveTab] = useOutletContext();
    useEffect(() => setActiveTab('users'), [setActiveTab]);
    return <FeatureLockedEmptyState feature="users" />;
};

export default UsersIndexPage;
