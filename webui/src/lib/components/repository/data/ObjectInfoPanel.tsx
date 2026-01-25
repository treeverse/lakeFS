import React from 'react';
import Table from 'react-bootstrap/Table';
import dayjs from 'dayjs';

import { ObjectEntry } from './DataBrowserContext';

interface ObjectInfoPanelProps {
    entry: ObjectEntry;
}

const humanSize = (bytes?: number): string => {
    if (!bytes) return '0.0 B';
    const e = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, e)).toFixed(1) + ' ' + ' KMGTP'.charAt(e) + 'B';
};

const EntryMetadata: React.FC<{ metadata: Record<string, string> }> = ({ metadata }) => {
    const keys = Object.getOwnPropertyNames(metadata);
    if (keys.length === 0) return null;

    return (
        <Table hover striped size="sm">
            <thead>
                <tr>
                    <th>Key</th>
                    <th>Value</th>
                </tr>
            </thead>
            <tbody>
                {keys.map((key) => (
                    <tr key={`metadata:${key}`}>
                        <td>
                            <code>{key}</code>
                        </td>
                        <td>
                            <code>{metadata[key]}</code>
                        </td>
                    </tr>
                ))}
            </tbody>
        </Table>
    );
};

export const ObjectInfoPanel: React.FC<ObjectInfoPanelProps> = ({ entry }) => {
    return (
        <div className="object-info-panel">
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
                    {entry.physical_address && (
                        <tr>
                            <td>
                                <strong>Physical Address</strong>
                            </td>
                            <td>
                                <code>{entry.physical_address}</code>
                            </td>
                        </tr>
                    )}
                    {entry.size_bytes !== undefined && (
                        <tr>
                            <td>
                                <strong>Size (Bytes)</strong>
                            </td>
                            <td>{`${entry.size_bytes}  (${humanSize(entry.size_bytes)})`}</td>
                        </tr>
                    )}
                    {entry.checksum && (
                        <tr>
                            <td>
                                <strong>Checksum</strong>
                            </td>
                            <td>
                                <code>{entry.checksum}</code>
                            </td>
                        </tr>
                    )}
                    {entry.mtime && (
                        <tr>
                            <td>
                                <strong>Last Modified</strong>
                            </td>
                            <td>
                                {`${dayjs.unix(entry.mtime).fromNow()} (${dayjs
                                    .unix(entry.mtime)
                                    .format('MM/DD/YYYY HH:mm:ss')})`}
                            </td>
                        </tr>
                    )}
                    {entry.content_type && (
                        <tr>
                            <td>
                                <strong>Content-Type</strong>
                            </td>
                            <td>
                                <code>{entry.content_type}</code>
                            </td>
                        </tr>
                    )}
                    {entry.metadata && Object.keys(entry.metadata).length > 0 && (
                        <tr>
                            <td>
                                <strong>Metadata</strong>
                            </td>
                            <td>
                                <EntryMetadata metadata={entry.metadata} />
                            </td>
                        </tr>
                    )}
                </tbody>
            </Table>
        </div>
    );
};
