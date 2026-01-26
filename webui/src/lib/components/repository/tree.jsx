import React from 'react';
import { FaDownload } from 'react-icons/fa';
import noop from 'lodash/noop';

import { Link } from '../nav';
import { RefTypeCommit } from '../../../constants';
import { ClipboardButton } from '../controls';

export const humanSize = (bytes) => {
    if (!bytes) return '0.0 B';
    const e = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, e)).toFixed(1) + ' ' + ' KMGTP'.charAt(e) + 'B';
};

function pathParts(path, isPathToFile) {
    let parts = path.split(/\//);
    let resolved = [];
    if (parts.length === 0) {
        return resolved;
    }

    if (parts[parts.length - 1] === '' || !isPathToFile) {
        parts = parts.slice(0, parts.length - 1);
    }

    for (let i = 0; i < parts.length; i++) {
        let currentPath = parts.slice(0, i + 1).join('/');
        if (currentPath.length > 0) {
            currentPath = `${currentPath}/`;
        }
        resolved.push({
            name: parts[i],
            path: currentPath,
        });
    }

    return resolved;
}

const buildPathURL = (params, query) => {
    return { pathname: '/repositories/:repoId/objects', params, query };
};

export const URINavigator = ({
    repo,
    reference,
    path,
    downloadUrl,
    relativeTo = '',
    pathURLBuilder = buildPathURL,
    isPathToFile = false,
    hasCopyButton = false,
}) => {
    const parts = pathParts(path, isPathToFile);
    const params = { repoId: repo.id };
    const displayedReference = reference.type === RefTypeCommit ? reference.id.substr(0, 12) : reference.id;

    return (
        <div className="d-flex">
            <div className="lakefs-uri flex-grow-1">
                <div title={displayedReference} className="w-100 text-nowrap overflow-hidden text-truncate">
                    {relativeTo === '' ? (
                        <>
                            <strong>lakefs://</strong>
                            <Link
                                href={{
                                    pathname: '/repositories/:repoId/objects',
                                    params,
                                    query: { ref: reference.id },
                                }}
                            >
                                {repo.id}
                            </Link>
                            <strong>/</strong>
                            <Link
                                href={{
                                    pathname: '/repositories/:repoId/objects',
                                    params,
                                    query: { ref: reference.id },
                                }}
                            >
                                {displayedReference}
                            </Link>
                            <strong>/</strong>
                        </>
                    ) : (
                        <>
                            <Link href={pathURLBuilder(params, { path: '' })}>{relativeTo}</Link>
                            <strong>/</strong>
                        </>
                    )}

                    {parts.map((part, i) => {
                        const path =
                            parts
                                .slice(0, i + 1)
                                .map((p) => p.name)
                                .join('/') + '/';
                        const query = { path, ref: reference.id };
                        const edgeElement =
                            isPathToFile && i === parts.length - 1 ? (
                                <span>{part.name}</span>
                            ) : (
                                <>
                                    <Link href={pathURLBuilder(params, query)}>{part.name}</Link>
                                    <strong>{'/'}</strong>
                                </>
                            );
                        return <span key={part.name}>{edgeElement}</span>;
                    })}
                </div>
            </div>
            <div className="object-viewer-buttons" style={{ flexShrink: 0 }}>
                {hasCopyButton && (
                    <ClipboardButton
                        text={`lakefs://${repo.id}/${reference.id}/${path}`}
                        variant="link"
                        size="sm"
                        onSuccess={noop}
                        onError={noop}
                        className={'me-1'}
                        tooltip={'copy URI to clipboard'}
                    />
                )}
                {downloadUrl && (
                    <a
                        href={downloadUrl}
                        download={path.split('/').pop()}
                        className="btn btn-link btn-sm download-button me-1"
                    >
                        <FaDownload />
                    </a>
                )}
            </div>
        </div>
    );
};
