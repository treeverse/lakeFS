import React, { useState, useEffect, useCallback } from 'react';
import Table from 'react-bootstrap/Table';
import Button from 'react-bootstrap/Button';
import Spinner from 'react-bootstrap/Spinner';
import Alert from 'react-bootstrap/Alert';
import ProgressBar from 'react-bootstrap/ProgressBar';
import { SyncIcon } from '@primer/octicons-react';

import { objects } from '../../../api';
import { ObjectEntry } from './DataBrowserContext';

interface ObjectStatisticsPanelProps {
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

export const ObjectStatisticsPanel: React.FC<ObjectStatisticsPanelProps> = ({ entry, repo, reference }) => {
    const [statistics, setStatistics] = useState<Statistics | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<Error | null>(null);
    const [progress, setProgress] = useState(0);
    const [calculating, setCalculating] = useState(false);

    const calculateStatistics = useCallback(async () => {
        setLoading(true);
        setCalculating(true);
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
            setCalculating(false);
        }
    }, [repo.id, reference.id, entry.path]);

    // Reset statistics when entry changes
    useEffect(() => {
        setStatistics(null);
        setError(null);
        setProgress(0);
    }, [entry.path]);

    if (!calculating && !statistics && !error) {
        return (
            <div className="object-statistics-panel p-4 text-center">
                <p className="text-muted mb-3">
                    Calculate the total number of objects and their cumulative size in this directory.
                </p>
                <Button variant="primary" onClick={calculateStatistics}>
                    <SyncIcon className="me-2" />
                    Calculate Statistics
                </Button>
            </div>
        );
    }

    if (loading) {
        return (
            <div className="object-statistics-panel p-4">
                <div className="text-center mb-3">
                    <Spinner animation="border" size="sm" className="me-2" />
                    <span>Calculating statistics...</span>
                </div>
                <p className="text-muted text-center mb-2">
                    Found {progress.toLocaleString()} objects so far. This may take a while for large directories.
                </p>
                <ProgressBar animated now={100} />
            </div>
        );
    }

    if (error) {
        return (
            <div className="object-statistics-panel p-3">
                <Alert variant="danger">Error calculating statistics: {error.message}</Alert>
                <div className="text-center">
                    <Button variant="outline-primary" onClick={calculateStatistics}>
                        <SyncIcon className="me-2" />
                        Try Again
                    </Button>
                </div>
            </div>
        );
    }

    if (statistics) {
        return (
            <div className="object-statistics-panel p-3">
                <Table responsive hover>
                    <tbody>
                        <tr>
                            <td>
                                <strong>Path</strong>
                            </td>
                            <td>
                                <code>{entry.path}</code>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <strong>Total Objects</strong>
                            </td>
                            <td>
                                <code>{statistics.totalObjects.toLocaleString()}</code>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <strong>Total Size</strong>
                            </td>
                            <td>
                                <code>
                                    {statistics.totalBytes.toLocaleString()} Bytes ({humanSize(statistics.totalBytes)})
                                </code>
                            </td>
                        </tr>
                    </tbody>
                </Table>
                <div className="text-center mt-3">
                    <Button variant="outline-secondary" size="sm" onClick={calculateStatistics}>
                        <SyncIcon className="me-2" />
                        Recalculate
                    </Button>
                </div>
            </div>
        );
    }

    return null;
};
