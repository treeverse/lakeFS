import React, {useState} from "react";
import {linkToPath} from "../actions/api";
import Alert from "react-bootstrap/Alert";
import Table from "react-bootstrap/Table";
import {
    ChevronDownIcon,
    ChevronUpIcon,
    DownloadIcon,
    PencilIcon,
    PlusIcon,
    CircleSlashIcon,
    TrashcanIcon
} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import OverlayTrigger from "react-bootstrap/OverlayTrigger";
import Tooltip from "react-bootstrap/Tooltip";
import Dropdown from "react-bootstrap/Dropdown";
import {connect} from "react-redux";
import {listBranches} from "../actions/branches";
import Card from "react-bootstrap/Card";

const PathLink = ({repoId, refId, path, children, as = null}) => {
    const link = linkToPath(repoId, refId.id, path);
    if (as === null) {
        return (<a href={link} download={true}>{children}</a>);
    }
    return React.createElement(as, {children: children, href: link, download: true});
};

const ChangeRowActions = ({repo, refId, entry, onDelete}) => {
    const [isDropdownOpen, setDropdownOpen] = useState(false);
    return (
        <Dropdown alignRight onToggle={setDropdownOpen}>
            <Dropdown.Toggle as={React.forwardRef(({onClick, children}, ref) => {
                return (
                    <Button variant="link" onClick={e => {
                        e.preventDefault();
                        onClick(e);
                    }} ref={ref}>
                        {children}
                    </Button>
                );
            })}>
                {isDropdownOpen ? <ChevronUpIcon/> : <ChevronDownIcon/>}
            </Dropdown.Toggle>

            <Dropdown.Menu>
                <PathLink path={entry.path} refId={refId} repoId={repo.id}
                          as={Dropdown.Item}><DownloadIcon/> {' '} Download</PathLink>
                <Dropdown.Item onClick={(e) => {
                    e.preventDefault();
                    if (window.confirm(`are you sure you wish to delete object "${entry.path}"?`)) onDelete(entry);
                }}><TrashcanIcon/> {' '} Delete
                </Dropdown.Item>
            </Dropdown.Menu>
        </Dropdown>
    );
};

const ChangeEntryRow = ({repo, refId, entry, onDelete, showActions}) => {
    let rowClass = 'tree-row ';
    switch (entry.type) {
        case 'changed':
            rowClass += 'diff-changed';
            break;
        case 'added':
            rowClass += 'diff-added';
            break;
        case 'removed':
            rowClass += 'diff-removed';
            break;
        case 'conflict':
            rowClass += 'diff-conflict';
            break;
        default:
            break;
    }

    const pathText = entry.path;

    let diffIndicator;
    switch (entry.type) {
        case 'removed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip>removed</Tooltip>)}>
                    <span>
                        <TrashcanIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'added':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip>added</Tooltip>)}>
                    <span>
                        <PlusIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'changed':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip>changed</Tooltip>)}>
                    <span>
                        <PencilIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        case 'conflict':
            diffIndicator = (
                <OverlayTrigger placement="bottom" overlay={(<Tooltip>conflict</Tooltip>)}>
                    <span>
                        <CircleSlashIcon/>
                    </span>
                </OverlayTrigger>
            );
            break;
        default:
            break;
    }

    let entryActions;
    if (showActions && entry.path_type === 'object') {
        entryActions = <ChangeRowActions repo={repo} refId={refId} entry={entry} onDelete={onDelete}></ChangeRowActions>;
    }

    return (
        <>
            <tr className={rowClass}>
                <td className="diff-indicator">
                    {diffIndicator}
                </td>
                <td className="tree-path">
                    <span>{pathText}</span>
                </td>
                <td className={"tree-row-actions"}>
                    {entryActions}
                </td>
            </tr>
        </>
    );
};

const Changes = ({list, repo, refId, onDelete, showActions}) => {
    const results = list.payload ? list.payload.results : [];
    let body;
    if (list.loading) {
        body = (<Alert variant="info">Loading...</Alert>);
    } else if (list.error) {
        body = <Alert variant="danger" className="tree-error">{list.error}</Alert>
    } else if (results.length === 0) {
        body = (<Alert variant="info">No changes</Alert>);
    } else {
        body = (
            <Table borderless size="sm">
                <tbody>
                {results.map(entry => (
                    <ChangeEntryRow
                        key={entry.path}
                        entry={entry}
                        repo={repo}
                        refId={refId}
                        onDelete={onDelete}
                        showActions={showActions}/>
                ))}
                </tbody>
            </Table>
        );
    }
    return (
        <div className="tree-container">
            <Card>
                {body}
            </Card>
        </div>
    );
};

export default connect(
    ({branches}) => ({
        listBranchesState: branches.list,
    }),
    ({listBranches})
)(Changes);
