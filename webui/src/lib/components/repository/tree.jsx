import React, {useCallback, useState} from "react";

import dayjs from "dayjs";
import {
    PasteIcon,
    DotIcon,
    DownloadIcon,
    FileDirectoryIcon,
    FileIcon, GearIcon, InfoIcon,
    PencilIcon,
    PlusIcon,
    TrashIcon, LogIcon
} from "@primer/octicons-react";
import Tooltip from "react-bootstrap/Tooltip";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";
import {OverlayTrigger} from "react-bootstrap";
import Button from "react-bootstrap/Button";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Dropdown from "react-bootstrap/Dropdown";

import {commits, linkToPath} from "../../api";
import {ConfirmationModal} from "../modals";
import {Paginator} from "../pagination";
import {Link} from "../nav";
import {RefTypeBranch, RefTypeCommit} from "../../../constants";
import {copyTextToClipboard, Error, Loading} from "../controls";
import Modal from "react-bootstrap/Modal";
import {useAPI} from "../../hooks/api";

export const humanSize = (bytes) => {
    if (!bytes) return '0.0 B';
    const e = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, e)).toFixed(1) + ' ' + ' KMGTP'.charAt(e) + 'B';
};

const Na = () => (<span>&mdash;</span>);

const EntryRowActions = ({ repo, reference, entry, onDelete }) => {

    const [showDeleteConfirmation, setShowDeleteConfirmation] = useState(false);
    const handleCloseDeleteConfirmation = () => setShowDeleteConfirmation(false);
    const handleShowDeleteConfirmation = () => setShowDeleteConfirmation(true);
    const deleteConfirmMsg = `are you sure you wish to delete object "${entry.path}"?`
    const onSubmitDeletion = () => {
        onDelete(entry);
        setShowDeleteConfirmation(false);
    };


    const [showObjectStat, setShowObjectStat] = useState(false);
    const [showObjectOrigin, setShowObjectOrigin] = useState(false);

    const handleShowObjectOrigin = useCallback((e) => {
        e.preventDefault();
        setShowObjectOrigin(true);
    }, [setShowObjectOrigin]);

    return (
        <>
            <Dropdown alignRight>
                <Dropdown.Toggle variant="light" size="sm" className={'row-hover'}>
                    <GearIcon/>
                </Dropdown.Toggle>

                <Dropdown.Menu>
                    {(entry.path_type === 'object') &&
                        <PathLink
                            path={entry.path}
                            reference={reference}
                            repoId={repo.id}
                            as={Dropdown.Item}>
                            <DownloadIcon/> {' '} Download
                        </PathLink>
                    }
                    {(entry.path_type === 'object') &&
                        <Dropdown.Item onClick={(e) => {
                            e.preventDefault();
                            setShowObjectStat(true);
                        }}>
                            <InfoIcon/> {' '} Object Info
                        </Dropdown.Item>
                    }


                    <Dropdown.Item onClick={handleShowObjectOrigin}>
                        <LogIcon/> Blame
                    </Dropdown.Item>

                    <Dropdown.Item onClick={(e) => {
                        copyTextToClipboard(`lakefs://${repo.id}/${reference.id}/${entry.path}`)
                        e.preventDefault()
                    }}>
                        <PasteIcon/> {' '} Copy URI
                    </Dropdown.Item>
                    {entry.path_type === 'object' && reference.type === RefTypeBranch &&
                        <>
                            <Dropdown.Divider />
                            <Dropdown.Item onClick={(e) => {
                                e.preventDefault();
                                handleShowDeleteConfirmation();
                            }}>
                                <TrashIcon/> {' '} Delete
                            </Dropdown.Item>
                        </>
                    }
                </Dropdown.Menu>
            </Dropdown>

            <ConfirmationModal
                show={showDeleteConfirmation}
                onHide={handleCloseDeleteConfirmation}
                msg={deleteConfirmMsg}
                onConfirm={onSubmitDeletion}/>

            <StatModal
                entry={entry}
                show={showObjectStat}
                onHide={() => setShowObjectStat(false)}/>

            <OriginModal
                entry={entry}
                repo={repo}
                reference={reference}
                show={showObjectOrigin}
                onHide={() => setShowObjectOrigin(false)}/>
        </>
    );
};


const StatModal = ({ show, onHide, entry }) => {
    return (
        <Modal show={show} onHide={onHide} size={"lg"}>
            <Modal.Header closeButton>
                <Modal.Title>Object Information</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <Table   hover >
                    <tbody>
                        <tr>
                            <td><strong>Path</strong></td>
                            <td><code>{entry.path}</code></td>
                        </tr>
                        <tr>
                            <td><strong>Physical Address</strong></td>
                            <td><code>{entry.physical_address}</code></td>
                        </tr>
                        <tr>
                            <td><strong>Size (Bytes)</strong></td>
                            <td>{`${entry.size_bytes}  (${humanSize(entry.size_bytes)})`}</td>
                        </tr>
                        <tr>
                            <td><strong>Checksum</strong></td>
                            <td><code>{entry.checksum}</code></td>
                        </tr>
                        <tr>
                            <td><strong>Last Modified</strong></td>
                            <td>{`${dayjs.unix(entry.mtime).fromNow()} (${dayjs.unix(entry.mtime).format("MM/DD/YYYY HH:mm:ss")})`}</td>
                        </tr>
                        {(entry.content_type) &&
                            <tr>
                                <td><strong>Content-Type</strong></td>
                                <td><code>{entry.content_type}</code></td>
                            </tr>
                        }
                    </tbody>
                </Table>
            </Modal.Body>
        </Modal>
    );
};


const CommitMetadata = ({ metadata }) => {
    const entries = Object.entries(metadata)
    if (entries.length === 0) {
        // empty state
        return <small>No metadata fields</small>
    }
    return (
        <Table striped>
            <tbody>
            {entries.map(([key, value]) =>  (
                <tr key={`blame-commit-md-${key}`}>
                   <td>{key}</td>
                   <td><code>{value}</code></td>
                </tr>
            ))}
            </tbody>
        </Table>
    )
}

const OriginModal = ({ show, onHide, entry, repo, reference }) => {

    const {response: commit, error, loading} = useAPI(async () => {
        if (show) {
            return await commits.blame(repo.id, reference.id, entry.path, entry.path_type);
        }
        return null

    }, [show, repo.id, reference.id, entry.path])

    const pathType = (entry.path_type === 'object') ? 'object' : 'prefix';

    let content = <Loading/>;

    if (error) {
        content = <Error error={error}/>
    }
    if (!loading && !error && commit) {
        content = (
            <>
                <Table hover >
                    <tbody>
                        <tr>
                            <td><strong>Path</strong></td>
                            <td><code>{entry.path}</code></td>
                        </tr>
                        <tr>
                            <td><strong>Commit ID</strong></td>
                            <td>
                                <Link className="mr-2" href={{
                                    pathname: '/repositories/:repoId/commits/:commitId',
                                    params: {repoId: repo.id, commitId: commit.id}
                                }}>
                                    <code>{commit.id}</code>
                                </Link>
                            </td>
                        </tr>
                        <tr>
                            <td><strong>Commit Message</strong></td>
                            <td>{commit.message}</td>
                        </tr>
                        <tr>
                            <td><strong>Committed By</strong></td>
                            <td>{commit.committer}</td>
                        </tr>
                        <tr>
                            <td><strong>Created At</strong></td>
                            <td>
                                <>{dayjs.unix(commit.creation_date).format("MM/DD/YYYY HH:mm:ss")}</> ({dayjs.unix(commit.creation_date).fromNow()})
                            </td>
                        </tr>
                        <tr>
                            <td><strong>Metadata</strong></td>
                            <td>
                                <CommitMetadata metadata={commit.metadata}/>
                            </td>
                        </tr>
                    </tbody>
                </Table>
            </>
        )
    }

    if (!loading && !error && !commit) {
        content = (
            <>
            <h5>
                <small>
                    No commit found, perhaps this is an
                    {' '}
                    <Link className="mr-2" href={{
                        pathname: '/repositories/:repoId/changes',
                        params: {repoId: repo.id},
                        query: {ref: reference.id},
                    }}>
                        uncommitted change
                    </Link>?
                </small>
            </h5>
            </>
        )
    }

    return (
        <Modal show={show} onHide={onHide} size={"lg"}>
            <Modal.Header closeButton>
                <Modal.Title>Last commit to modify <>{pathType}</></Modal.Title>
            </Modal.Header>
            <Modal.Body>
                {content}
            </Modal.Body>
        </Modal>
    );
};


const PathLink = ({repoId, reference, path, children, as = null}) => {
    const name = path.split('/').pop()
    const link = linkToPath(repoId, reference.id, path);
    if (as === null)
        return (<a href={link} download={name}>{children}</a>);
    return React.createElement(as, {href: link, download: name}, children);
};

const EntryRow = ({repo, reference, path, entry, onDelete, showActions}) => {
    let rowClass = 'change-entry-row ';
    switch (entry.diff_type) {
        case 'changed':
            rowClass += 'diff-changed';
            break;
        case 'added':
            rowClass += 'diff-added';
            break;
        case 'removed':
            rowClass += 'diff-removed';
            break;
        default:
            break;
    }

    const buttonText = (path.length > 0) ? entry.path.substr(path.length) : entry.path;

    const params = {repoId: repo.id};
    const query = { ref: reference.id, path: entry.path};

    let button;
    if (entry.path_type === 'common_prefix') {
        button = (<Link href={{pathname: '/repositories/:repoId/objects', query, params}}>{buttonText}</Link>);
    } else if (entry.diff_type === 'removed') {
        button = (<span>{buttonText}</span>);
    } else {
        const filePathQuery = {
            ref: query.ref,
            path: query.path,
        }
        button = (<Link href={{pathname: '/repositories/:repoId/object', query: filePathQuery, params: params}}>{buttonText}</Link>);
    }

    let size;
    if (entry.diff_type === 'removed' || entry.path_type === 'common_prefix') {
        size = (<Na/>);
    } else {
        size = (
            <OverlayTrigger placement="bottom" overlay={<Tooltip>{entry.size_bytes} bytes</Tooltip>}>
                <span>{humanSize(entry.size_bytes)}</span>
            </OverlayTrigger>
        );
    }

    let modified;
    if (entry.diff_type === 'removed' || entry.path_type === 'common_prefix') {
        modified = (<Na/>);
    } else {
        modified = (
            <OverlayTrigger placement="bottom"
                            overlay={<Tooltip>{dayjs.unix(entry.mtime).format("MM/DD/YYYY HH:mm:ss")}</Tooltip>}>
                <span>{dayjs.unix(entry.mtime).fromNow()}</span>
            </OverlayTrigger>
        );
    }

    let diffIndicator;
    switch (entry.diff_type) {
        case 'removed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip>removed in diff</Tooltip>)}>
                    <span>
                        <TrashIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'added':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip>added in diff</Tooltip>)}>
                    <span>
                        <PlusIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'changed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip>changed in diff</Tooltip>)}>
                    <span>
                        <PencilIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        default:
            break;
    }

    let entryActions;
    if (showActions && (entry.diff_type !== 'removed')) {
        entryActions = <EntryRowActions repo={repo} reference={reference} entry={entry} onDelete={onDelete}/>;
    }

    return (
        <>
            <tr className={rowClass}>
                <td className="diff-indicator">
                    {diffIndicator}
                </td>
                <td className="tree-path">
                    {entry.path_type === 'common_prefix' ? <FileDirectoryIcon/> : <FileIcon/>} {' '}
                    {button}
                </td>
                <td className="tree-size">
                    {size}
                </td>
                <td className="tree-modified">
                    {modified}
                </td>
                <td className={"change-entry-row-actions"}>
                    {entryActions}
                </td>
            </tr>
        </>
    );
};


function pathParts(path) {
    let parts = path.split(/\//);
    let resolved = [];
    if (parts.length === 0) {
        return resolved;
    }

    if (parts[parts.length - 1] === "") {
        parts = parts.slice(0, parts.length - 1);
    }

    // else
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
    return {pathname: '/repositories/:repoId/objects', params, query};
};

export const URINavigator = ({ repo, reference, path, relativeTo = "", pathURLBuilder = buildPathURL, isPathToFile = false }) => {
    const parts = pathParts(path);
    const params = {repoId: repo.id};

    return (
        <span className="lakefs-uri">
            {(relativeTo === "") ? (
                <>
                    <strong>{'lakefs://'}</strong>
                    <Link href={{pathname: '/repositories/:repoId/objects', params}}>{repo.id}</Link>
                    <strong>{'/'}</strong>
                    <Link href={{pathname: '/repositories/:repoId/objects',params, query: {ref: reference.id}}}>{(reference.type === RefTypeCommit) ? reference.id.substr(0, 12) : reference.id}</Link>
                    <strong>{'/'}</strong>
                </>
            ): (

                <>
                    <Link href={pathURLBuilder(params, {path: ""})}>{relativeTo}</Link>
                    <strong>{'/'}</strong>
                </>
            )}

            {parts.map((part, i) => {
                const path = parts.slice(0, i+1).map(p => p.name).join('/') + '/';
                const query = {path, ref: reference.id};
                const edgeElement = isPathToFile && i === parts.length - 1 ?
                    (
                        <span>
                            {part.name}
                        </span>
                    ) :
                    (
                        <>
                            <Link href={pathURLBuilder(params, query)}>{part.name}</Link>
                            <strong>{'/'}</strong>
                        </>
                    );
                return (
                    <span key={part.name}>
                        {edgeElement}
                    </span>
                );
            })}
        </span>
    );
};

const GetStarted = ({ config, onUpload, onImport }) => {
    return (
        <Container className="m-4 mb-5">
            <h2 className="mt-2">To get started with this repository:</h2>

            <Row className="pt-2 ml-2">
                <DotIcon className="mr-1 mt-1"/>
                <Button variant="link" disabled={(config.config.blockstore_type === 'local' || config.config.blockstore_type === 'mem')} onClick={onImport}>Import</Button>&nbsp;data from {config.config.blockstore_type}. Or, see the&nbsp;<a href="https://docs.lakefs.io/setup/import.html" target="_blank" rel="noopener noreferrer">docs</a>&nbsp;for other ways to import data to your repository.
            </Row>

            <Row className="pt-2 ml-2">
                <DotIcon className="mr-1 mt-1"/>
                <Button variant="link" onClick={onUpload}>Upload</Button>&nbsp;an object.
            </Row>

            <Row className="pt-2 ml-2">
                <DotIcon className="mr-1 mt-1"/>
                Use&nbsp;<a href="https://docs.lakefs.io/integrations/distcp.html" target="_blank" rel="noopener noreferrer">DistCp</a>&nbsp;or&nbsp;
                <a href="https://docs.lakefs.io/integrations/rclone.html" target="_blank" rel="noopener noreferrer">Rclone</a>&nbsp;to copy data into your repository.
            </Row>
        </Container>
    );
};

export const Tree = ({ config, repo, reference, results, after, onPaginate, nextPage, onUpload, onImport, onDelete, showActions = false, path = "" }) => {

    let body;
    if (results.length === 0 && path === "" && reference.type === RefTypeBranch) {
        // empty state!
        body = <GetStarted config={config} onUpload={onUpload} onImport={onImport}/>;
    } else {
        body = (
            <>
                <Table borderless size="sm">
                    <tbody>
                    {results.map(entry => (
                        <EntryRow
                            key={entry.path}
                            entry={entry}
                            path={path}
                            repo={repo}
                            reference={reference}
                            showActions={showActions}
                            onDelete={onDelete}
                        />
                    ))}
                    </tbody>
                </Table>
            </>
        );
    }

    return (
        <div className="tree-container">
            <Card>
                <Card.Header>
                    <URINavigator path={path} repo={repo} reference={reference}/>
                </Card.Header>
                <Card.Body>
                    {body}
                </Card.Body>
            </Card>

            <Paginator onPaginate={onPaginate} nextPage={nextPage} after={after}/>
        </div>
    );
};
