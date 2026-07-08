import React from 'react';

import Container from 'react-bootstrap/Container';

import { DatasetsNavTabs } from '../../lib/components/datasets/tabs';
import { DatasetsEnterpriseInfo } from '../../lib/components/datasets/datasetsEnterpriseInfo';

const DatasetsPage = () => {
    return (
        <div>
            <div className="full-width-tabs-border">
                <DatasetsNavTabs />
            </div>
            <Container fluid="sm">
                <div className="mt-4">
                    <DatasetsEnterpriseInfo />
                </div>
            </Container>
        </div>
    );
};

export default DatasetsPage;
