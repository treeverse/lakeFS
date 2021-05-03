import React, {useState} from "react";

import moment from "moment";
import {
    DotIcon,
    DownloadIcon,
    FileDirectoryIcon,
    FileIcon,
    PencilIcon,
    PlusIcon,
    TrashIcon
} from "@primer/octicons-react";
import Tooltip from "react-bootstrap/Tooltip";
import Table from "react-bootstrap/Table";
import Card from "react-bootstrap/Card";
import {OverlayTrigger} from "react-bootstrap";
import Button from "react-bootstrap/Button";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Dropdown from "react-bootstrap/Dropdown";

import {linkToPath} from "../../api";
import {ConfirmationModal} from "../modals";
import {Paginator} from "../pagination";
import {Link} from "../nav";


const humanSize = (bytes) => {
    if (!bytes) return '0.0 B';
    const e = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, e)).toFixed(1) + ' ' + ' KMGTP'.charAt(e) + 'B';
};

const Na = () => (<span>&mdash;</span>);

const EntryRowActions = ({ repo, reference, entry, onDelete }) => {
    const [show, setShow] = useState(false);
    const handleClose = () => setShow(false);;
    const handleShow = () => setShow(true);
    const deleteConfirmMsg = `are you sure you wish to delete object "${entry.path}"?`
    const onSubmit = () => {
        onDelete(entry);
        setShow(false);
    };
    return (
        <>
            <Dropdown alignRight>
                <Dropdown.Toggle variant="light" size="sm">
                    More Actions
                </Dropdown.Toggle>

                <Dropdown.Menu>
                    <PathLink
                        path={entry.path}
                        reference={reference}
                        repoId={repo.id}
                        as={Dropdown.Item}>
                        <DownloadIcon/> {' '} Download
                    </PathLink>
                    <Dropdown.Item onClick={(e) => {
                        e.preventDefault();
                        handleShow();
                    }}>
                        <TrashIcon/> {' '} Delete
                    </Dropdown.Item>
                </Dropdown.Menu>
            </Dropdown>

            <ConfirmationModal show={show} onHide={handleClose} msg={deleteConfirmMsg} onConfirm={onSubmit}/>
        </>
    );
};

const PathLink = ({repoId, reference, path, children, as = null}) => {
    const link = linkToPath(repoId, reference.id, path);
    if (as === null)
        return (<a href={link} download={true}>{children}</a>);
    return React.createElement(as, {href: link, download: true}, children);
};

const EntryRow = ({repo, reference, path, entry, onDelete, showActions}) => {
    let rowClass = 'tree-row ';
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
        button = (<PathLink repoId={repo.id} reference={reference} path={entry.path}>{buttonText}</PathLink>);
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
                            overlay={<Tooltip>{moment.unix(entry.mtime).format("MM/DD/YYYY HH:mm:ss")}</Tooltip>}>
                <span>{moment.unix(entry.mtime).fromNow()}</span>
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
    if (showActions && entry.path_type === 'object' && (entry.diff_type !== 'removed')) {
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
                <td className={"tree-row-actions"}>
                    {entryActions}
                </td>
            </tr>
        </>
    );
};


function pathParts(path, rootName = "root") {
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

const URINavigator = ({ repo, reference, path }) => {
    const parts = pathParts(path);
    const params = {repoId: repo.id};

    return (
        <span className="lakefs-uri">
            <strong>{'lakefs://'}</strong>
            <Link href={{pathname: '/repositories/:repoId/objects', params}}>{repo.id}</Link>
            <strong>{'/'}</strong>
            <Link href={{pathname: '/repositories/:repoId/objects',params, query: {ref: reference.id}}}>{(reference.type === 'commit') ? reference.id.substr(0, 12) : reference.id}</Link>
            <strong>{'/'}</strong>
            {parts.map((part, i) => {
                const path = parts.slice(0, i+1).map(p => p.name).join('/') + '/';
                const query = {path, ref: reference.id};
                return (
                    <span key={i}>
                        <Link href={{pathname: '/repositories/:repoId/objects', params, query}}>{part.name}</Link>
                        <strong>{'/'}</strong>
                    </span>
                );
            })}
        </span>
    );
};

const GetStarted = ({ onUpload }) => {
    return (
        <Container className="m-4 mb-5">
            <h2 className="mt-2">To get started with this repository, you can:</h2>

            <Row className="pt-2 ml-2">
                <DotIcon className="mr-1 mt-1"/>
                <Button variant="link" onClick={onUpload}>Upload</Button>&nbsp;an object.
            </Row>

            <Row className="pt-2 ml-2">
                <DotIcon className="mr-1 mt-1"/>
                See the &nbsp;<a href="https://docs.lakefs.io/using/" target="_blank" rel="noopener noreferrer">docs</a>&nbsp;for other ways to import data to your repository.
            </Row>
        </Container>
    );
};

export const Tree = ({ repo, reference, results, after, onPaginate, nextPage, onUpload, onDelete, showActions = false, path = "" }) => {

    let body;
    if (results.length === 0 && path === "") {
        // empty state!
        body = <GetStarted onUpload={onUpload}/>;
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