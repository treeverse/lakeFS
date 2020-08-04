import React, {useCallback, useEffect, useState} from "react";
import {linkToPath} from "../actions/api";
import Alert from "react-bootstrap/Alert";
import Table from "react-bootstrap/Table";
import {
    ChevronDownIcon,
    ChevronUpIcon,
    DotIcon,
    DownloadIcon,
    FileDirectoryIcon,
    FileIcon,
    PencilIcon,
    PlusIcon,
    TrashcanIcon
} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import * as moment from "moment";
import Card from "react-bootstrap/Card";
import {Link} from "react-router-dom";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Dropdown from "react-bootstrap/Dropdown";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import {connect} from "react-redux";
import {listBranches} from "../actions/branches";


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
            {refWithPath("", refId.id)}
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
    const link = linkToPath(repoId, refId.id, path);
    if (as === null) {
        return  (<a href={link} download={true}>{children}</a>);
    }
    return React.createElement(as, {children: children, href: link, download: true});
};


const Na = () => (<span>&mdash;</span>);

const EntryRowActions = ({ repo, refId, entry, onDelete }) => {
    const [isDropdownOpen, setDropdownOpen] = useState(false);
    return (
        <Dropdown alignRight onToggle={setDropdownOpen}>
        <Dropdown.Toggle as={React.forwardRef(({onClick, children}, ref) => {
                return (
                    <Button variant="link" onClick={e => { e.preventDefault(); onClick(e); }} ref={ref}>
                    {children}
                    </Button>
                );
            })}>
            {isDropdownOpen ? <ChevronUpIcon/> : <ChevronDownIcon/>}
        </Dropdown.Toggle>

        <Dropdown.Menu>
            <PathLink path={entry.path} refId={refId} repoId={repo.id} as={Dropdown.Item}><DownloadIcon/> {' '} Download</PathLink>
            <Dropdown.Item onClick={(e) => {
                    e.preventDefault();
                    if (window.confirm(`are you sure you wish to delete object "${entry.path}"?`)) onDelete(entry);
                }}><TrashcanIcon/> {' '} Delete
            </Dropdown.Item>
        </Dropdown.Menu>
        </Dropdown>
    );
};

const EntryRow = ({ repo, refId, path, entry, onNavigate, onDelete, showActions }) => {
    let rowClass = 'tree-row ';
    switch(entry.diff_type) {
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

    let button;
    if (entry.diff_type === 'removed') {
        button = (<span>{buttonText}</span>);
    } else if (entry.path_type === 'common_prefix') {
        button = (<Link onClick={(e) => { onNavigate(entry.path); e.preventDefault() }} to="#">{buttonText}</Link>);
    } else {
        button = (<PathLink path={entry.path} refId={refId} repoId={repo.id}>{buttonText}</PathLink>);
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
            <OverlayTrigger placement="bottom" overlay={<Tooltip>{moment.unix(entry.mtime).format("MM/DD/YYYY HH:mm:ss")}</Tooltip>}>
                <span>{moment.unix(entry.mtime).fromNow()}</span>
            </OverlayTrigger>
        );
    }

    let diffIndicator;
    switch(entry.diff_type) {
        case 'removed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip>removed in diff</Tooltip>)}>
                    <span>
                        <TrashcanIcon/>
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
        entryActions = <EntryRowActions repo={repo} refId={refId} entry={entry} onDelete={onDelete}></EntryRowActions>;
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


const merge = (path, entriesAtPath, diffResults) => {
    diffResults = (diffResults && diffResults.payload) ? diffResults.payload.results : [];
    entriesAtPath = entriesAtPath || [];

    const entries = [...entriesAtPath];
    diffResults.forEach(diff => {
       if (isChildOf(path, diff.path) && diff.type === 'removed') {
           entries.push({
               path: diff.path,
               path_type: diff.path_type,
               diff_type: 'removed'
           });
       }
    });

    const r = entries.map(entry => {
        if (entry.diff_type) return entry;

        for (let i=0; i < diffResults.length; i++) {
            const diff = diffResults[i];
            if (entry.path === diff.path) {
                // if there's an exact 'CHANGE' or 'ADD' diff for it, color it that way.
                return {...entry,  diff_type: diff.type};
            }
            if (diff.path_type === 'common_prefix' && isDescendantOf(diff.path, entry.path) &&  diff.type === 'added') {
                // for any entry descendant from a TREE event that was ADD, color it as ADD
                return {...entry, diff_type: 'added'};
            }

            if (entry.path_type === 'common_prefix' && isDescendantOf(entry.path, diff.path)) {
                // for any TREE that has CHANGE/ADD/REMOVE descendants, color it a CHANGE
                return {...entry, diff_type: 'changed'};
            }
        }
        return {...entry, diff_type: 'NONE'};
    });
    return r;
};

const Tree = ({ path, list, repo, refId, diffResults, onNavigate, onDelete, showActions, listBranches, listBranchesState, setShowUploadModal, setShowImportModal }) => {
    let body;
    const refreshData = useCallback(() => {
        if (refId.type === 'branch') {
            if (refId.id === repo.default_branch) {
                listBranches(repo.id, "", 2) // trigger list branches to
            }
        }
    }, [repo.id, listBranches, refId, repo.default_branch]);
    useEffect(() => {
        refreshData();
    }, [refreshData, repo.id, refId, path]);

    const showGetStarted = !list.loading && list.payload && list.payload.results.length === 0 && listBranchesState && listBranchesState.payload && listBranchesState.payload.results.length === 1 && !path;

    if (list.loading) {
        body = (<Alert variant="info">Loading...</Alert>);
    } else if (list.error) {
        body = <Alert variant="danger" className="tree-error">{list.error}</Alert>
    } else if (showGetStarted) {
        body = <GetStarted repo={repo} list={list} listBranchesState={listBranchesState}
                           setShowUploadModal={setShowUploadModal} setShowImportModal={setShowImportModal}/>
    } else {
        const results = merge(path, list.payload.results, diffResults);
        body = (
            <Table borderless size="sm">
                <tbody>
                {results.map(entry => (
                    <EntryRow key={entry.path} entry={entry} onNavigate={onNavigate} path={path} repo={repo} refId={refId} onDelete={onDelete} showActions={showActions}/>
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


const GetStarted = ({repo, list, listBranchesState, setShowUploadModal, setShowImportModal}) => {
    useEffect(() =>{
    }, [repo, list, listBranchesState])
    return <>{ (
                <Container className="m-3"><h3>To get started with this repository, you can:</h3>
                <Row className="pt-2 ml-2" xs="0"><DotIcon className="mr-1 mt-1"/><a href="/#" onClick={(e) => {e.preventDefault(); setShowImportModal(true)}}>Import</a>&nbsp;data from S3 without copying it.</Row>
                <Row className="pt-2 ml-2" ><DotIcon className="mr-1 mt-1"/><a href="/#" onClick={(e) => {e.preventDefault(); setShowUploadModal(true)}}>Upload</a>&nbsp;an object.</Row>
                <Row className="pt-2 ml-2" ><DotIcon className="mr-1 mt-1"/>See the&nbsp;<a href="https://docs.lakefs.io/using/" target="_blank" rel="noopener noreferrer">docs</a>&nbsp;for other ways to import data to your repository.</Row>
</Container>    )}</>
}

export default connect(
    ({branches}) => ({
        listBranchesState: branches.list,
    }),
    ({listBranches})
)(Tree);
