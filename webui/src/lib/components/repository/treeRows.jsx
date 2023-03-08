import React, {useState} from "react";
import {Link} from "../nav";
import {
    ChevronDownIcon,
    ChevronRightIcon, CircleSlashIcon,
    ClockIcon,
    FileDirectoryIcon,
    HistoryIcon, PencilIcon, PlusIcon, TableIcon, TrashIcon, DiffIcon
} from "@primer/octicons-react";
import ChangeSummary from "./ChangeSummary";
import {ConfirmationModal} from "../modals";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";
import Button from "react-bootstrap/Button";
import {TreeRowType} from "../../../constants";

class RowAction {
    /**
     * @param {JSX.Element} icon
     * @param {string} text
     * @param {()=>void} onClick
     */
    constructor(icon, text, onClick) {
        this.icon = icon
        this.text = text
        this.onClick = onClick
    }
}

/**
 * @param {[RowAction]} actions
 */
const ChangeRowActions = ({actions}) => <>
    {
        actions.map(action => (
            <Button variant="link" disabled={false}
                  onClick={(e) => {
                      e.preventDefault();
                      action.onClick()
                  }}>
                {action.icon
                    ? action.icon
                    : action.text}
            </Button>
        ))}
</>;

export const ObjectTreeEntryRow = ({entry, relativeTo = "", diffExpanded, depth = 0, loading = false, onRevert, onClickExpandDiff = null}) => {
    const [showRevertConfirm, setShowRevertConfirm] = useState(false)
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathSection = extractPathText(entry, relativeTo);
    const diffIndicator = <DiffIndicationIcon entry={entry} rowType={TreeRowType.Object}/>;

    const rowActions = []
    if (onClickExpandDiff) {
        rowActions.push(new RowAction(null, diffExpanded ? "Hide object changes" : "Show object changes", onClickExpandDiff))
    }
    if (onRevert) {
        rowActions.push(new RowAction(<HistoryIcon/>, "Revert changes", () => {
            setShowRevertConfirm(true)
        }))
    }
    return (
        <TableRow className={rowClass} entry={entry} diffIndicator={diffIndicator} rowActions={rowActions}
                  onRevert={onRevert} depth={depth} loading={loading} pathSection={pathSection}
                  showRevertConfirm={showRevertConfirm} setShowRevertConfirm={() => setShowRevertConfirm(false)}/>
    );
};

export const PrefixTreeEntryRow = ({entry, relativeTo = "", dirExpanded, depth = 0, onClick, loading = false, onRevert, onNavigate, getMore}) => {
    const [showRevertConfirm, setShowRevertConfirm] = useState(false)
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathSection = extractPathText(entry, relativeTo);
    let diffIndicator = <DiffIndicationIcon entry={entry} rowType={TreeRowType.Prefix}/>;
    const [showSummary, setShowSummary] = useState(false);
    if (entry.path_type === "common_prefix") {
        pathSection = <Link href={onNavigate(entry)}>{pathSection}</Link>
    }
    const rowActions = []
    rowActions.push(new RowAction(null, showSummary ? "Hide change summary" : "Calculate change summary", () => setShowSummary(!showSummary)))
    if (onRevert) {
        rowActions.push(new RowAction(<HistoryIcon/>, "Revert changes", () => {
            setShowRevertConfirm(true)
        }))
    }

    return (
        <TableRow className={rowClass} entry={entry} diffIndicator={diffIndicator} getMore={getMore} rowActions={rowActions}
                  onRevert={onRevert} depth={depth} loading={loading} pathSection={pathSection} showSummary={showSummary}
                  dirExpanded={dirExpanded} onExpand={onClick}
                  showRevertConfirm={showRevertConfirm} setShowRevertConfirm={() => setShowRevertConfirm(false)}
        />
    );
};

export const TableTreeEntryRow = ({entry, relativeTo = "", onClickExpandDiff, depth = 0, loading = false, onRevert}) => {
    const [showRevertConfirm, setShowRevertConfirm] = useState(false)
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathSection = extractTableName(entry, relativeTo);
    const diffIndicator = <DiffIndicationIcon entry={entry} rowType={TreeRowType.Table}/>

    const rowActions = []
    rowActions.push(new RowAction(null, "Show table changes", onClickExpandDiff))
    if (onRevert) {
        rowActions.push(new RowAction(<HistoryIcon/>, "Revert changes", () => {
            setShowRevertConfirm(true)
        }))
    }
    return (
        <TableRow className={rowClass} entry={entry} diffIndicator={diffIndicator} rowActions={rowActions}
                  onRevert={onRevert} depth={depth} loading={loading} pathSection={pathSection}
                  showRevertConfirm={showRevertConfirm} setShowRevertConfirm={() => setShowRevertConfirm(false)}/>
    );
};

const PrefixExpansionSection = ({dirExpanded, onClick}) => {
    return (<span onClick={onClick}>
                {dirExpanded ? <ChevronDownIcon/> : <ChevronRightIcon/>}
            </span>)
}

const TableRow = ({diffIndicator, depth, loading, showSummary, entry, getMore, rowActions,
                      showRevertConfirm, setShowRevertConfirm, pathSection, onRevert, dirExpanded, onExpand, ...rest}) => {
    return (<tr {...rest}>
            <td className="entry-type-indicator">{diffIndicator}</td>
            <td className="tree-path">
                        <span style={{marginLeft: (depth * 20) + "px"}}>
                            {onExpand && <PrefixExpansionSection dirExpanded={dirExpanded} onClick={onExpand}/>}
                            {loading ? <ClockIcon/> : ""}
                            {pathSection}
                        </span>
            </td>
            <td className={"change-summary"}>{showSummary && <ChangeSummary prefix={entry.path} getMore={getMore}/>}</td>
            <td className={"change-entry-row-actions"}>
                <ChangeRowActions actions={rowActions} />
                <ConfirmationModal show={showRevertConfirm} onHide={setShowRevertConfirm}
                                   msg={`Are you sure you wish to revert "${entry.path}" (${entry.type})?`}
                                   onConfirm={() => onRevert(entry)}/>
            </td>
        </tr>
    )
}

function extractPathText(entry, relativeTo) {
    let pathText = entry.path;
    if (pathText.startsWith(relativeTo)) {
        pathText = pathText.substr(relativeTo.length);
    }
    return pathText;
}

function diffType(entry) {
    switch (entry.type) {
        case 'changed':
        case 'prefix_changed':
            return 'diff-changed';
        case 'added':
            return 'diff-added';
        case 'removed':
            return 'diff-removed';
        case 'conflict':
            return 'diff-conflict';
        default:
            return '';
    }
}

function extractTableName(entry, relativeTo) {
    let pathText = entry.path;
    if (pathText.startsWith(relativeTo)) {
        pathText = pathText.substr(relativeTo.length);
    }
    if (pathText.endsWith("/")) {
        pathText = pathText.slice(0,-1)
    }
    return pathText;
}

export const DiffIndicationIcon = ({entry, rowType}) => {
    let diffIcon;
    let tooltipId;
    let tooltipText;
    if (rowType === TreeRowType.Prefix) {
        diffIcon = <FileDirectoryIcon/>;
        tooltipId = "tooltip-prefix";
        tooltipText = "Changes under prefix";
    } else if (rowType === TreeRowType.Table) {
        diffIcon = <TableIcon/>;
        tooltipId = "tooltip-table";
        tooltipText = "Table changed"
    } else {
        switch (entry.type) {
            case 'removed':
                diffIcon = <TrashIcon/>;
                tooltipId = "tooltip-removed";
                tooltipText = "Removed";
                break;
            case 'added':
                diffIcon = <PlusIcon/>;
                tooltipId = "tooltip-added";
                tooltipText = "Added";
                break;
            case 'changed':
                diffIcon = <PencilIcon/>;
                tooltipId = "tooltip-changed";
                tooltipText = "Changed";
                break;
            case 'conflict':
                diffIcon = <CircleSlashIcon/>;
                tooltipId = "tooltip-conflict";
                tooltipText = "Conflict";
                break;
            default:
        }
    }

    return <OverlayTrigger placement="bottom" overlay={(<Tooltip id={tooltipId}>{tooltipText}</Tooltip>)}>
                <span>
                    {diffIcon}
                </span>
    </OverlayTrigger>;
}
