import React, { useState, useCallback, useEffect } from 'react';
import Table from 'react-bootstrap/Table';
import Button from 'react-bootstrap/Button';
import Spinner from 'react-bootstrap/Spinner';
import Alert from 'react-bootstrap/Alert';
import { SyncIcon, CopyIcon, CheckIcon } from '@primer/octicons-react';

import { objects } from '../../../api';
import { ObjectEntry } from './DataBrowserContext';

interface DirectoryInfoPanelProps {
    entry: ObjectEntry;
    repo: { id: string };
    reference: { id: string; type: string };
}

const humanSize = (bytes: number): string => {
    if (!bytes) return '0.0 B';
    const e = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, e)).toFixed(1) + ' ' + ' KMGTP'.charAt(e) + 'B';
};

interface Statistics {
    totalObjects: number;
    totalBytes: number;
}

export const DirectoryInfoPanel: React.FC<DirectoryInfoPanelProps> = ({ entry, repo, reference }) => {
    const [statistics, setStatistics] = useState<Statistics | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<Error | null>(null);
    const [progress, setProgress] = useState(0);
    const [copied, setCopied] = useState(false);

    const copyToClipboard = useCallback(async () => {
        try {
            await navigator.clipboard.writeText(entry.path);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        } catch (err) {
            console.error('Failed to copy to clipboard:', err);
        }
    }, [entry.path]);

    const calculateStatistics = useCallback(async () => {
        setLoading(true);
        setError(null);
        setProgress(0);
        setStatistics(null);

        try {
            let totalObjects = 0;
            let totalBytes = 0;
            let finished = false;

            const iterator = objects.listAll(repo.id, reference.id, entry.path);

            while (!finished) {
                const { page, done } = await iterator.next();
                for (const obj of page) {
                    totalObjects++;
                    totalBytes += obj.size_bytes || 0;
                }
                setProgress(totalObjects);
                if (done) finished = true;
            }

            setStatistics({ totalObjects, totalBytes });
        } catch (err) {
            setError(err instanceof Error ? err : new Error('Failed to calculate statistics'));
        } finally {
            setLoading(false);
        }
    }, [repo.id, reference.id, entry.path]);

    // Reset statistics when entry changes
    useEffect(() => {
        setStatistics(null);
        setError(null);
        setProgress(0);
        setLoading(false);
    }, [entry.path]);

    const renderStatisticsValue = () => {
        if (loading) {
            return (
                <span className="text-muted">
                    <Spinner animation="border" size="sm" className="me-2" style={{ width: 12, height: 12 }} />
                    Calculating... ({progress.toLocaleString()} objects found)
                </span>
            );
        }

        if (error) {
            return (
                <span className="text-danger">
                    Failed to calculate.{' '}
                    <Button variant="link" size="sm" className="p-0" onClick={calculateStatistics}>
                        Retry
                    </Button>
                </span>
            );
        }

        if (statistics) {
            return (
                <>
                    <code>{statistics.totalObjects.toLocaleString()}</code> objects,{' '}
                    <code>{humanSize(statistics.totalBytes)}</code>
                    <span className="text-muted ms-1">({statistics.totalBytes.toLocaleString()} bytes)</span>
                    <Button
                        variant="link"
                        size="sm"
                        className="p-0 ms-2 text-muted"
                        onClick={calculateStatistics}
                        title="Recalculate"
                    >
                        <SyncIcon size={12} />
                    </Button>
                </>
            );
        }

        return (
            <Button variant="link" size="sm" className="p-0" onClick={calculateStatistics}>
                <SyncIcon size={12} className="me-1" />
                Calculate
            </Button>
        );
    };

    return (
        <div className="directory-info-panel">
            {error && (
                <Alert variant="danger" className="mb-3" dismissible onClose={() => setError(null)}>
                    {error.message}
                </Alert>
            )}

            <Table responsive className="info-table">
                <tbody>
                    <tr>
                        <td className="info-label">
                            <strong>Path</strong>
                        </td>
                        <td>
                            <code>{entry.path}</code>
                            <Button
                                variant="link"
                                size="sm"
                                className="p-0 ms-2"
                                onClick={copyToClipboard}
                                title={copied ? 'Copied!' : 'Copy to clipboard'}
                            >
                                {copied ? <CheckIcon size={14} className="text-success" /> : <CopyIcon size={14} />}
                            </Button>
                        </td>
                    </tr>
                    <tr>
                        <td className="info-label">
                            <strong>Type</strong>
                        </td>
                        <td>Directory</td>
                    </tr>
                    <tr>
                        <td className="info-label">
                            <strong>Statistics</strong>
                        </td>
                        <td>{renderStatisticsValue()}</td>
                    </tr>
                </tbody>
            </Table>
        </div>
    );
};
