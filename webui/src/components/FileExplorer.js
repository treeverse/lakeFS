import React from 'react';
import Table from "react-bootstrap/Table";
import Octicon, {FileBinary, FileDirectory} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";

import "./FileExplorer.css";
import * as moment from "moment";

const parentOf = (path) => {
    const parts = path.split(/\//);
    const parentParts = parts.slice(0, parts.length-2);
    return parentParts.join('/') + '/';
};

const humanSize = (bytes) => {
    if (!bytes) return '0.00 B';
    const e = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes/Math.pow(1024, e)).toFixed(2)+' '+' KMGTP'.charAt(e)+'B';
}

const Na = () => (<span>&mdash;</span>);

export const FileExplorer = ({ path, entries, loading, onNavigate }) => {

    return (
        <Table hover className="file-explorer">
            <thead>
                <tr>
                    <th>Path</th>
                    <th>Last Modified</th>
                    <th>Size</th>
                    <th>Content Hash</th>
                </tr>
            </thead>
            <tbody>
                <tr key={".."}>
                    <td>
                        <Button variant="link" onClick={(e) => {
                            e.preventDefault();
                            onNavigate(parentOf(path));
                        }}>
                            <strong>..</strong>
                        </Button>
                    </td>
                    <td><Na/></td>
                    <td><Na/></td>
                    <td><Na/></td>
                </tr>
                {entries.map(entry => (
                <tr key={entry.path}>
                    <td>
                        <Button variant="file-explorer-link" onClick={(e) => {
                            e.preventDefault();
                            if (entry.path_type === 'TREE') {
                                onNavigate(entry.path);
                            }
                        }}>
                            <Octicon icon={(entry.path_type === 'TREE') ? FileDirectory : FileBinary}/> {' '}
                            {(path.length > 0) ? entry.path.substr(path.length) : entry.path}
                        </Button>
                    </td>
                    <td>
                        {(entry.path_type === 'OBJECT') ?
                            <span>{moment.unix(entry.mtime).toString()} ({moment.unix(entry.mtime).fromNow()})</span> :
                            <Na/>
                        }
                    </td>
                    <td>
                        {(entry.path_type === 'OBJECT') ?
                            <span>{entry.size_bytes} ({humanSize(entry.size_bytes)})</span> :
                            <Na/>
                        }
                    </td>
                    <td>
                        {(entry.path_type === 'OBJECT') ?
                            <span>{entry.checksum}</span> :
                            <Na/>
                        }
                    </td>
                </tr>
                ))}
            </tbody>
        </Table>
    );
};
