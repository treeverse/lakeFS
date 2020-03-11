import React, {useEffect, useState} from "react";
import {linkToPath} from "../actions/api";
import Alert from "react-bootstrap/Alert";
import Table from "react-bootstrap/Table";
import Octicon, {
    File,
    FileDirectory,
    Plus,
    Trashcan,
    Pencil,
    ChevronDown,
    ChevronUp,
    CloudDownload
} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import * as moment from "moment";
import Card from "react-bootstrap/Card";
import {Link} from "react-router-dom";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Dropdown from "react-bootstrap/Dropdown";


const humanSize = (bytes) => {
    if (!bytes) return '0.0 B';
    const e = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes/Math.pow(1024, e)).toFixed(1)+' '+' KMGTP'.charAt(e)+'B';
};


function pathParts(path, rootName = "root") {
    let parts = path.split(/\//);
    let resolved = [];
    if (parts.length === 0) {
        return resolved;
    }

    if (parts[parts.length-1] === "") {
        parts = parts.slice(0, parts.length-1);
    }

    // else
    for (let i=0; i<parts.length; i++) {
        let currentPath = parts.slice(0, i+1).join('/');
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

const isChildOf = (path, child) => {
    if (child.indexOf(path) !== 0) return false;
    return child
        .substr(path.length)
        .split(/\//)
        .filter(part => part.length > 0).length === 1;
};

const isDescendantOf = (path, child) => {
    if (child.indexOf(path) !== 0) return false;
    return child
        .substr(path.length)
        .split(/\//)
        .filter(part => part.length > 0).length >= 1;
};


const URINavigator = ({ repo, refId, path, onNavigate }) => {
    const parts = pathParts(path);
    // decide if commit or not?

    const qs = (dict) => {
        const qs = new URLSearchParams();
        Object.getOwnPropertyNames(dict).forEach(k => {
            if (!!dict[k]) {
                qs.set(k, dict[k]);
            }
        });
        let query =  qs.toString();
        if (query.length > 0) {
            return `?${query}`
        }
        return query;
    };

    const baseUrl = `/repositories/${repo.id}/tree`;

    const refIdDisplay = (refId.type === 'commit') ? refId.id.substr(0, 16) : refId.id;

    const refWithPath = (path, name) => {
        const refQuery = (refId.type === 'commit') ? qs({path, commit: refId.id}) : qs({path, branch: refId.id});
        const refUrl = `${baseUrl}${refQuery}`;
        return (<Link to={refUrl} onClick={(e) => { e.preventDefault(); onNavigate(path) }}>{name}</Link>);
    };

    return (
        <span className="lakefs-uri">
            <strong>{'lakefs://'}</strong>
            <Link to={baseUrl}>{repo.id}</Link>
            <strong>{'@'}</strong>
            {refWithPath("", refIdDisplay)}
            <strong>{'/'}</strong>
            {parts.map((part, i) => (
                <span key={i}>
                    {refWithPath(part.path, part.name)}
                    <strong>{'/'}</strong>
                </span>
            ))}
        </span>
    );
};


const PathLink = ({ repoId, refId, path, children, as = null }) => {
    const [link, setLink] = useState("#");

    useEffect( () => {
        let mounted = true;
        linkToPath(repoId, refId.id, path).then(link => {
            if (mounted)
                setLink(link);
        });
        return () => { mounted = false };
    }, [repoId, refId.id, path]);
    if (as === null) {
        return  (<a href={link}>{children}</a>);
    }

    return React.createElement(as, {children: children, href: link});
};


const Na = () => (<span>&mdash;</span>);

const EntryRow = ({ repo, refId, path, entry, onNavigate, onDelete }) => {

    const [isDropdownOpen, setDropdownOpen] = useState(false);

    let rowClass = 'tree-row ';
    if (entry.diff_type === 'CHANGED') {
        rowClass += 'diff-changed';
    } else if (entry.diff_type === 'ADDED') {
        rowClass += 'diff-added';
    } else if (entry.diff_type === 'REMOVED') {
        rowClass += 'diff-removed';
    }

    const buttonText = (path.length > 0) ? entry.path.substr(path.length) : entry.path;

    let button;
    if (entry.diff_type === 'REMOVED') {
        button = (<span>{buttonText}</span>);
    } else if (entry.path_type === 'TREE') {
        button = (<Button variant="link" onClick={() => {
            onNavigate(entry.path)
        }}>{buttonText}</Button>)
    } else {
        button = (<PathLink path={entry.path} refId={refId} repoId={repo.id}>{buttonText}</PathLink>);
    }

    let size;
    if (entry.diff_type === 'REMOVED') {
        size = (<Na/>);
    } else {
        size = (
            <OverlayTrigger placement="bottom" overlay={<Tooltip>{entry.size_bytes} bytes</Tooltip>}>
                <span>{humanSize(entry.size_bytes)}</span>
            </OverlayTrigger>
        );
    }

    let modified;
    if (entry.diff_type === 'REMOVED') {
        modified = (<Na/>);
    } else {
        modified = (
            <OverlayTrigger placement="bottom" overlay={<Tooltip>{moment.unix(entry.mtime).format("MM/DD/YYYY HH:mm:ss")}</Tooltip>}>
                <span>{moment.unix(entry.mtime).fromNow()}</span>
            </OverlayTrigger>
        );
    }

    let diffIndicator;
    if (entry.diff_type === 'REMOVED') {
        diffIndicator = (
            <OverlayTrigger placement="bottom" overlay={(<Tooltip>removed in diff</Tooltip>)}>
                <span>
                    <Octicon icon={Trashcan}/>
                </span>
            </OverlayTrigger>
        );
    } else if (entry.diff_type === 'ADDED') {
        diffIndicator = (
            <OverlayTrigger placement="bottom" overlay={(<Tooltip>added in diff</Tooltip>)}>
                <span>
                    <Octicon icon={Plus}/>
                </span>
            </OverlayTrigger>
        );
    } else if (entry.diff_type === 'CHANGED') {
        diffIndicator = (
            <OverlayTrigger placement="bottom" overlay={(<Tooltip>changed in diff</Tooltip>)}>
                <span>
                    <Octicon icon={Pencil}/>
                </span>
            </OverlayTrigger>
        );
    } else {
        diffIndicator = (<span/>);
    }

    let deleter = (<span/>);
    if (entry.path_type === 'OBJECT' && (entry.diff_type !== 'REMOVED')) {
        deleter = (
            <Dropdown alignRight onToggle={setDropdownOpen}>
                <Dropdown.Toggle
                    as={React.forwardRef(({onClick, children}, ref) => {
                        return (
                            <Button variant="link" onClick={(e) => { e.preventDefault(); onClick(e); }} ref={ref}>
                                {children}
                            </Button>
                            );
                    })}>
                    <Octicon icon={(isDropdownOpen) ?  ChevronUp : ChevronDown}/>
                </Dropdown.Toggle>

                <Dropdown.Menu>
                    <PathLink path={entry.path} refId={refId} repoId={repo.id} as={Dropdown.Item}><Octicon icon={CloudDownload}/> {' '} Download</PathLink>
                    <Dropdown.Item onClick={(e) => { e.preventDefault(); if (window.confirm(`are  you sure you wish to delete object "${entry.path}"?`)) onDelete(entry); }}><Octicon icon={Trashcan}/> {' '} Delete</Dropdown.Item>
                </Dropdown.Menu>
            </Dropdown>
        );
    }

    return (
        <tr className={rowClass}>
            <td className="diff-indicator">
                {diffIndicator}
            </td>
            <td className="tree-path">
                <Octicon icon={entry.path_type === 'TREE' ? FileDirectory : File}/> {' '}
                {button}
            </td>
            <td className="tree-size">
                {size}
            </td>
            <td className="tree-modified">
                {modified}
            </td>
            <td className={"tree-row-actions"}>
                {deleter}
            </td>
        </tr>
    );
};


const merge = (path, entriesAtPath, diffResults) => {
    diffResults = (!!diffResults && !!diffResults.payload) ? diffResults.payload.results : [];
    entriesAtPath = entriesAtPath || [];


    const entries = [...entriesAtPath];
    diffResults.forEach(diff => {
       if (isChildOf(path, diff.path) && diff.type === 'REMOVED') {
           entries.push({
               path: diff.path,
               path_type: diff.path_type,
               diff_type: 'REMOVED'
           });
       }
    });

    return entries.map(entry => {
        if (!!entry.diff_type) return entry;

        for (let i=0; i < diffResults.length; i++) {
            const diff = diffResults[i];
            if (entry.path === diff.path) {
                // if there's an exact 'CHANGE' or 'ADD' diff for it, color it that way.
                return {...entry,  diff_type: diff.type};

            }
            if (diff.path_type === 'TREE' && isDescendantOf(diff.path, entry.path) &&  diff.type === 'ADDED') {
                // for any entry descendant from a TREE event that was ADD, color it as ADD
                return {...entry, diff_type: 'ADDED'};
            }

            if (entry.path_type === 'TREE' && isDescendantOf(entry.path, diff.path)) {
                // for any TREE that has CHANGE/ADD/REMOVE descendants, color it a CHANGE
                return {...entry, diff_type: 'CHANGED'};
            }
        }
        return {...entry, diff_type: 'NONE'};
    });
};

export default ({ path, list, repo, refId, diffResults, onNavigate, onDelete }) => {
    let body;
    if (list.loading) {
        body = (<Alert variant="info">Loading...</Alert>);
    } else if (!!list.error) {
        body = <Alert variant="danger" className="tree-error">{list.error}</Alert>
    } else {
        const results = merge(path, list.payload.results, diffResults);
        body = (
            <Table borderless size="sm">
                <tbody>
                {results.map(entry => (
                    <EntryRow key={entry.path} entry={entry} onNavigate={onNavigate} path={path} repo={repo} refId={refId} onDelete={onDelete}/>
                ))}
                </tbody>
            </Table>
        );
    }

    return (
        <div className="tree-container">
            <Card>
                <Card.Header>
                    <URINavigator path={path} repo={repo} refId={refId} onNavigate={onNavigate}/>
                </Card.Header>
                <Card.Body>
                    {body}
                </Card.Body>
            </Card>
        </div>
    );
};
