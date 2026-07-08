import React, { FC } from 'react';
import Card from 'react-bootstrap/Card';
import { LockIcon } from '@primer/octicons-react';

const DATASETS_DOCS_URL = 'https://docs.lakefs.io/datamanagement/datasets';
const BOOK_DEMO_URL = 'https://lakefs.io/book-a-demo/';

export const DatasetsEnterpriseInfo: FC = () => {
    return (
        <Card>
            <div className="datasets-gate-content">
                <div className="datasets-gate-icon">
                    <LockIcon size={24} />
                </div>
                <h4>Datasets is an Enterprise feature</h4>
                <p className="text-muted">
                    Use lakeFS Datasets to version and share curated data collections across your organization.
                </p>
                <div className="datasets-gate-actions">
                    <a href={BOOK_DEMO_URL} target="_blank" rel="noopener noreferrer" className="btn btn-primary">
                        Book a demo
                    </a>
                    <a
                        href={DATASETS_DOCS_URL}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="btn btn-outline-secondary"
                    >
                        Learn more
                    </a>
                </div>
            </div>
        </Card>
    );
};
