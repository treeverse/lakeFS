import React, { FC } from 'react';

import Modal from 'react-bootstrap/Modal';
import Button from 'react-bootstrap/Button';
import {
    PeopleIcon,
    PersonIcon,
    KeyIcon,
    ShieldLockIcon,
    SparkleFillIcon,
    ChecklistIcon,
    VerifiedIcon,
    GlobeIcon,
} from '@primer/octicons-react';

type OcticonComponent = React.FC<{ size?: number | string }>;

const BOOK_DEMO_URL = 'https://lakefs.io/book-a-demo/';
const ENTERPRISE_DOCS_URL = 'https://docs.lakefs.io/latest/enterprise/';

const ENTERPRISE_BULLETS: Array<{ title: string; description: string; icon: OcticonComponent }> = [
    {
        title: 'RBAC',
        description: 'Fine-grained permissions',
        icon: ShieldLockIcon,
    },
    {
        title: 'SSO',
        description: 'SAML, OIDC, LDAP & AD',
        icon: GlobeIcon,
    },
    {
        title: 'Audit Logs',
        description: 'Track every operation',
        icon: ChecklistIcon,
    },
    {
        title: 'External Auth',
        description: 'Delegate to your IAM',
        icon: PersonIcon,
    },
    {
        title: 'Support & SLA',
        description: 'Enterprise-grade uptime',
        icon: VerifiedIcon,
    },
];

export type EnterpriseFeatureKey = 'credentials' | 'users' | 'groups' | 'policies';

const FEATURE_COPY: Record<EnterpriseFeatureKey, { headline: string; subhead: string }> = {
    credentials: {
        headline: 'Manage access keys with lakeFS Enterprise',
        subhead: 'Issue, rotate, and revoke per-user credentials.',
    },
    users: {
        headline: 'Manage users with lakeFS Enterprise',
        subhead: 'Invite teammates, provision API users, and govern their access.',
    },
    groups: {
        headline: 'Manage groups with lakeFS Enterprise',
        subhead: 'Organize users into teams and grant permissions in bulk.',
    },
    policies: {
        headline: 'Define policies with lakeFS Enterprise',
        subhead: 'IAM-style permissions for repositories, branches, and paths.',
    },
};

interface EnterpriseUpgradeModalProps {
    feature: EnterpriseFeatureKey;
    show: boolean;
    onHide: () => void;
}

export const EnterpriseUpgradeModal: FC<EnterpriseUpgradeModalProps> = ({ feature, show, onHide }) => {
    const copy = FEATURE_COPY[feature];

    return (
        <Modal
            show={show}
            onHide={onHide}
            size="lg"
            centered
            dialogClassName="enterprise-upgrade-dialog"
            contentClassName="enterprise-upgrade-content"
        >
            <Modal.Header closeButton closeVariant="white" className="enterprise-upgrade-hero">
                <div className="enterprise-upgrade-hero-inner">
                    <div className="enterprise-upgrade-badge">
                        <SparkleFillIcon size={12} />
                        <span>lakeFS Enterprise</span>
                    </div>
                    <h4 className="enterprise-upgrade-headline">{copy.headline}</h4>
                    <p className="enterprise-upgrade-subhead">{copy.subhead}</p>
                </div>
            </Modal.Header>
            <Modal.Body className="p-4">
                <div className="enterprise-upgrade-grid">
                    {ENTERPRISE_BULLETS.map((b) => {
                        const Icon = b.icon;
                        return (
                            <div key={b.title} className="enterprise-upgrade-pill">
                                <span className="enterprise-upgrade-pill-icon">
                                    <Icon size={16} />
                                </span>
                                <span>
                                    <strong className="d-block">{b.title}</strong>
                                    <span className="text-muted small">{b.description}</span>
                                </span>
                            </div>
                        );
                    })}
                </div>
            </Modal.Body>
            <Modal.Footer className="enterprise-upgrade-footer">
                <a
                    href={ENTERPRISE_DOCS_URL}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="me-auto small text-muted text-decoration-none"
                >
                    Learn more →
                </a>
                <Button
                    variant="success"
                    href={BOOK_DEMO_URL}
                    target="_blank"
                    rel="noopener noreferrer"
                    onClick={onHide}
                    className="enterprise-upgrade-cta"
                >
                    Unlock Access Control
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

interface FeatureLockedEmptyStateProps {
    feature: EnterpriseFeatureKey;
}

interface FeatureCopy {
    icon: React.ComponentType<{ size?: number | string }>;
    headline: string;
    description: ReactNode;
    learnMore: { href: string; label: string };
    primaryActions: Array<{ label: string; variant: string }>;
    tableHeaders: string[];
    placeholderRows: string[][];
}

const FEATURE_DETAILS: Record<EnterpriseFeatureKey, FeatureCopy> = {
    credentials: {
        icon: KeyIcon,
        headline: 'Your access keys live here',
        description: (
            <>
                Access keys are the credentials applications use to authenticate against lakeFS. Create, rotate, and
                revoke per-user keys when an authentication backend is configured.
            </>
        ),
        learnMore: {
            href: 'https://docs.lakefs.io/latest/security/sso/',
            label: 'Learn about lakeFS authentication',
        },
        primaryActions: [{ label: 'Create Access Key', variant: 'success' }],
        tableHeaders: ['Access Key ID', 'Issued At', 'Status'],
        placeholderRows: [
            ['AKIAIOSFODNN7EXAMPLE', '2 days ago', 'Active'],
            ['AKIAI44QH8DHBEXAMPLE', '3 weeks ago', 'Active'],
            ['AKIAJ7Z2K9XQEXAMPLE0', '6 months ago', 'Revoked'],
        ],
    },
    users: {
        icon: PersonIcon,
        headline: 'Bring your team into lakeFS',
        description: (
            <>
                Users are the people and applications that access lakeFS. Invite teammates, provision API users, and
                manage their lifecycle from one place.
            </>
        ),
        learnMore: {
            href: 'https://docs.lakefs.io/latest/security/rbac/',
            label: 'Learn about lakeFS users',
        },
        primaryActions: [
            { label: 'Invite User', variant: 'primary' },
            { label: 'Create User', variant: 'success' },
        ],
        tableHeaders: ['User ID', 'Email', 'Created At'],
        placeholderRows: [
            ['jane.doe', 'jane.doe@example.com', 'Mar 12, 2025'],
            ['data-pipeline', '—', 'Feb 28, 2025'],
            ['acme-spark', '—', 'Jan 04, 2025'],
        ],
    },
    groups: {
        icon: PeopleIcon,
        headline: 'Organize access with groups',
        description: (
            <>
                Groups bundle users together so you can grant permissions to a whole team at once instead of managing
                each user individually.
            </>
        ),
        learnMore: {
            href: 'https://docs.lakefs.io/latest/security/rbac/',
            label: 'Learn about groups and RBAC',
        },
        primaryActions: [{ label: 'Create Group', variant: 'success' }],
        tableHeaders: ['Group Name', 'Members', 'Created At'],
        placeholderRows: [
            ['Admins', '4', 'Mar 12, 2025'],
            ['DataEngineering', '12', 'Feb 28, 2025'],
            ['Analysts', '23', 'Jan 04, 2025'],
        ],
    },
    policies: {
        icon: ShieldLockIcon,
        headline: 'Define fine-grained permissions',
        description: (
            <>
                Policies are IAM-style documents that describe what actions users and groups are allowed to perform on
                which repositories, branches, and paths.
            </>
        ),
        learnMore: {
            href: 'https://docs.lakefs.io/latest/security/rbac/',
            label: 'Learn about policies',
        },
        primaryActions: [{ label: 'Create Policy', variant: 'success' }],
        tableHeaders: ['Policy ID', 'Statements', 'Created At'],
        placeholderRows: [
            ['FullAccess', '4', 'Mar 12, 2025'],
            ['ReadOnlyDataLake', '2', 'Feb 28, 2025'],
            ['BranchProtection', '3', 'Jan 04, 2025'],
        ],
    },
};

export const FeatureLockedEmptyState: FC<FeatureLockedEmptyStateProps> = ({ feature }) => {
    const [showModal, setShowModal] = React.useState(false);
    const details = FEATURE_DETAILS[feature];
    const Icon = details.icon;

    return (
        <>
            <div className="feature-locked-empty-state">
                <div className="feature-locked-actions mb-3 d-flex flex-wrap gap-2">
                    {details.primaryActions.map((action) => (
                        <Button key={action.label} variant={action.variant} onClick={() => setShowModal(true)}>
                            {action.label}
                        </Button>
                    ))}
                </div>

                <div className="feature-locked-table-preview" aria-hidden="true">
                    <table className="table feature-locked-table">
                        <thead>
                            <tr>
                                {details.tableHeaders.map((h) => (
                                    <th key={h}>{h}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {details.placeholderRows.map((row, i) => (
                                <tr key={i}>
                                    {row.map((cell, j) => (
                                        <td key={j}>{cell}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>

                <div className="feature-locked-card p-4 rounded text-center mt-3">
                    <div className="feature-locked-icon mb-3" aria-hidden="true">
                        <Icon size={32} />
                    </div>
                    <h4 className="mb-2">{details.headline}</h4>
                    <p className="text-muted mb-3">{details.description}</p>
                    <div className="d-flex flex-wrap justify-content-center gap-2 mb-1">
                        <Button variant="success" onClick={() => setShowModal(true)}>
                            Unlock Access Control
                        </Button>
                        <Button
                            variant="outline-secondary"
                            href={details.learnMore.href}
                            target="_blank"
                            rel="noopener noreferrer"
                        >
                            {details.learnMore.label}
                        </Button>
                    </div>
                </div>
            </div>

            <EnterpriseUpgradeModal feature={feature} show={showModal} onHide={() => setShowModal(false)} />
        </>
    );
};
