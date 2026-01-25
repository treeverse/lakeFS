import React, { useState, useCallback } from 'react';
import Table from 'react-bootstrap/Table';
import Button from 'react-bootstrap/Button';
import Spinner from 'react-bootstrap/Spinner';
import Alert from 'react-bootstrap/Alert';
import dayjs from 'dayjs';
import { CopyIcon, CheckIcon } from '@primer/octicons-react';

import { commits } from '../../../api';
import { useAPI } from '../../../hooks/api';
import { ObjectEntry } from './DataBrowserContext';
import { Link } from '../../nav';

interface ObjectBlamePanelProps {
    entry: ObjectEntry;
    repo: { id: string };
    reference: { id: string; type: string };
}

export const ObjectBlamePanel: React.FC<ObjectBlamePanelProps> = ({ entry, repo, reference }) => {
    const [copiedId, setCopiedId] = useState(false);

    const {
        response: commit,
        error,
        loading,
    } = useAPI(async () => {
        return await commits.blame(repo.id, reference.id, entry.path, entry.path_type);
    }, [repo.id, reference.id, entry.path, entry.path_type]);

    const copyCommitId = useCallback(async () => {
        if (!commit) return;
        try {
            await navigator.clipboard.writeText(commit.id);
            setCopiedId(true);
            setTimeout(() => setCopiedId(false), 2000);
        } catch (err) {
            console.error('Failed to copy to clipboard:', err);
        }
    }, [commit]);

    if (loading) {
        return (
            <div className="object-blame-panel p-3">
                <Table responsive className="info-table">
                    <tbody>
                        <tr>
                            <td className="info-label">
                                <strong>Last Modified By</strong>
                            </td>
                            <td>
                                <Spinner
                                    animation="border"
                                    size="sm"
                                    className="me-2"
                                    style={{ width: 12, height: 12 }}
                                />
                                <span className="text-muted">Loading...</span>
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </div>
        );
    }

    if (error) {
        return (
            <div className="object-blame-panel p-3">
                <Alert variant="danger">Error loading blame information: {(error as Error).message}</Alert>
            </div>
        );
    }

    if (!commit) {
        return (
            <div className="object-blame-panel p-3">
                <Table responsive className="info-table">
                    <tbody>
                        <tr>
                            <td className="info-label">
                                <strong>Last Modified By</strong>
                            </td>
                            <td>
                                <span className="text-muted">
                                    No commit found. This may be an{' '}
                                    <Link
                                        href={{
                                            pathname: '/repositories/:repoId/objects',
                                            params: { repoId: repo.id },
                                            query: { ref: reference.id, showChanges: 'true' },
                                        }}
                                    >
                                        uncommitted change
                                    </Link>
                                    .
                                </span>
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </div>
        );
    }

    return (
        <div className="object-blame-panel p-3">
            <Table responsive className="info-table">
                <tbody>
                    <tr>
                        <td className="info-label">
                            <strong>Commit ID</strong>
                        </td>
                        <td>
                            <Link
                                href={{
                                    pathname: '/repositories/:repoId/commits/:commitId',
                                    params: { repoId: repo.id, commitId: commit.id },
                                }}
                            >
                                <code>{commit.id.substring(0, 12)}</code>
                            </Link>
                            <Button
                                variant="link"
                                size="sm"
                                className="p-0 ms-2"
                                onClick={copyCommitId}
                                title={copiedId ? 'Copied!' : 'Copy full commit ID'}
                            >
                                {copiedId ? <CheckIcon size={14} className="text-success" /> : <CopyIcon size={14} />}
                            </Button>
                        </td>
                    </tr>
                    <tr>
                        <td className="info-label">
                            <strong>Message</strong>
                        </td>
                        <td>{commit.message || <span className="text-muted">No message</span>}</td>
                    </tr>
                    <tr>
                        <td className="info-label">
                            <strong>Committer</strong>
                        </td>
                        <td>{commit.committer}</td>
                    </tr>
                    <tr>
                        <td className="info-label">
                            <strong>Date</strong>
                        </td>
                        <td>
                            {dayjs.unix(commit.creation_date).format('MMM D, YYYY HH:mm:ss')}
                            <span className="text-muted ms-2">({dayjs.unix(commit.creation_date).fromNow()})</span>
                        </td>
                    </tr>
                    {commit.parents && commit.parents.length > 0 && (
                        <tr>
                            <td className="info-label">
                                <strong>Parents</strong>
                            </td>
                            <td>
                                {commit.parents.map((parent: string, index: number) => (
                                    <span key={parent}>
                                        {index > 0 && ', '}
                                        <Link
                                            href={{
                                                pathname: '/repositories/:repoId/commits/:commitId',
                                                params: { repoId: repo.id, commitId: parent },
                                            }}
                                        >
                                            <code>{parent.substring(0, 12)}</code>
                                        </Link>
                                    </span>
                                ))}
                            </td>
                        </tr>
                    )}
                </tbody>
            </Table>
        </div>
    );
};
