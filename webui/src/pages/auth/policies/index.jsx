import React, { useEffect } from 'react';
import { useOutletContext } from 'react-router-dom';
import { FeatureLockedEmptyState } from '../../../lib/components/auth/enterpriseUpgrade';

const PoliciesPage = () => {
    const [setActiveTab] = useOutletContext();
    useEffect(() => setActiveTab('policies'), [setActiveTab]);
    return <FeatureLockedEmptyState feature="policies" />;
};

export default PoliciesPage;
