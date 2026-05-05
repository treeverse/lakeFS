import React from 'react';
import Card from 'react-bootstrap/Card';
import { TableIcon } from '@primer/octicons-react';

export const TablesEnterpriseInfo = () => {
    return (
        <div className="tree-container tree-container-wide">
            <Card>
                <div className="tables-info-content">
                    <div className="tables-info-icon">
                        <TableIcon size={24} />
                    </div>
                    <h4>Tables is an Enterprise feature</h4>
                    <p className="text-muted">
                        lakeFS Enterprise provides an Iceberg REST catalog to manage and explore Iceberg tables directly
                        from lakeFS.
                    </p>
                    <a
                        href="https://lakefs.io/enterprise/"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="btn btn-primary"
                    >
                        Try lakeFS Enterprise
                    </a>
                    <a
                        href="https://docs.lakefs.io/understand/enterprise/"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="btn btn-secondary"
                    >
                        Learn More
                    </a>
                </div>
            </Card>
        </div>
    );
};
