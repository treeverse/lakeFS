import React, {useEffect, useState} from "react";
import {linkToPath} from "../actions/api";
import Alert from "react-bootstrap/Alert";
import Table from "react-bootstrap/Table";
import Octicon, {File, FileDirectory} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import * as moment from "moment";
import Card from "react-bootstrap/Card";
import {Link} from "react-router-dom";


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

const URINavigator = ({ repoId, branchId, path }) => {
    const parts = pathParts(path);
    return (
        <span className="lakefs-uri">
            <strong>{'lakefs://'}</strong>
            <Link to={`/repositories/${repoId}/tree`}>{repoId}</Link>
            <strong>{'@'}</strong>
            <Link to={`/repositories/${repoId}/tree?branch=${branchId}`}>{branchId}</Link>
            <strong>{'/'}</strong>
            {parts.map((part, i) => (
                <span key={i}>
                    <Link to={`/repositories/${repoId}/tree?branch=${branchId}&path=${part.path}`}>{part.name}</Link>
                    <strong>{'/'}</strong>
                </span>
            ))}
        </span>
    );
};

const PathLink = ({ repoId, branchId, path, name }) => {
    const [link, setLink] = useState("#");

    useEffect( () => {
        let mounted = true;
        linkToPath(repoId, branchId, path).then(link => {
            if (mounted)
                setLink(link);
        });
        return () => { mounted = false };
    }, [repoId, branchId, path, name]);
    return (
        <a href={link}>{name}</a>
    );
};

const Na = () => (<span>&mdash;</span>);


export default ({ path, list, repoId, branchId, onNavigate }) => {
    let body;
    if (list.loading) {
        body = (<Alert variant="info">Loading...</Alert>);
    } else if (!!list.error) {
        body = <Alert variant="danger" className="tree-error">{list.error}</Alert>
    } else {
        body = (
            <Table borderless hover size="sm">
                <tbody>
                {list.payload.results.map(entry => (
                    <tr key={entry.path}>
                        <td className="tree-path">
                            <Octicon icon={entry.path_type === 'TREE' ? FileDirectory : File}/> {' '}
                            {entry.path_type === 'TREE' ?
                                <Button variant="link" onClick={() => {
                                    onNavigate(entry.path)
                                }}>
                                    {(path.length > 0) ? entry.path.substr(path.length) : entry.path}
                                </Button> :
                                <PathLink path={entry.path} branchId={branchId} repoId={repoId} name={(path.length > 0) ? entry.path.substr(path.length) : entry.path}/>
                            }
                        </td>
                        <td className="tree-size">
                            {(entry.path_type === 'OBJECT') ?
                                <span>{humanSize(entry.size_bytes)}</span> :
                                <Na/>
                            }
                        </td>
                        <td className="tree-modified">
                            {(entry.path_type === 'OBJECT') ?
                                <span>{moment.unix(entry.mtime).fromNow()}</span> :
                                <Na/>
                            }
                        </td>

                    </tr>
                ))}
                </tbody>
            </Table>
        );
    }

    return (
        <div className="tree-container">
            <Card>
                <Card.Header>
                    <URINavigator path={path} repoId={repoId} branchId={branchId} onNavigate={onNavigate}/>
                </Card.Header>
                <Card.Body>
                    {body}
                </Card.Body>
            </Card>
        </div>
    );
};
