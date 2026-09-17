import React, { useEffect } from 'react';
import { useAuthOutletContext } from '../../../lib/components/auth/layout';
import { FeatureLockedEmptyState } from '../../../lib/components/auth/enterpriseUpgrade';

export const GroupsPage = () => {
    const [setActiveTab] = useAuthOutletContext();
    useEffect(() => setActiveTab('groups'), [setActiveTab]);
    return <FeatureLockedEmptyState feature="groups" />;
};

export default GroupsPage;
