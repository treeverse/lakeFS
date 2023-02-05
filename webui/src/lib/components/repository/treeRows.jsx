import React, {useState} from "react";
import {Link} from "../nav";
import {
    ChevronDownIcon,
    ChevronRightIcon, CircleSlashIcon,
    ClockIcon,
    FileDiffIcon, FileDirectoryIcon,
    GraphIcon,
    HistoryIcon, PencilIcon, PlusIcon, TableIcon, TrashIcon, DiffIcon
} from "@primer/octicons-react";
import ChangeSummary from "./ChangeSummary";
import {ConfirmationModal} from "../modals";
import {OverlayTrigger} from "react-bootstrap";
import Tooltip from "react-bootstrap/Tooltip";

class RowAction {
    /**
     * @param {JSX.Element} icon
     * @param {string} tooltip
     * @param {boolean} visible
     * @param {()=>void} onClick
     */
    constructor(icon, tooltip, visible, onClick) {
        this.tooltip = tooltip
        this.visible = visible
        this.onClick = onClick
        this.icon = icon
    }
}

/**
 * @param {[RowAction]} actions
 */
const ChangeRowActions = ({actions}) => <>
    {
        actions.map(action => (
            <><OverlayTrigger placement="bottom" overlay={<Tooltip>{action.tooltip}</Tooltip>}>
                <Link className={"btn-link"} disabled={false} style={{visibility: action.visible ? "visible" : ""}}
                      onClick={(e) => {
                          e.preventDefault();
                          action.onClick()
                      }}>
                    {action.icon}
                </Link>
            </OverlayTrigger>&#160;&#160;</>
        ))}
</>;

export const ObjectTreeEntryRow = ({entry, relativeTo = "", diffExpanded, depth = 0, loading = false, onRevert, onClickExpandDiff = null}) => {
    const [showRevertConfirm, setShowRevertConfirm] = useState(false)
    let rowClass = 'tree-entry-row ' + diffType(entry);
    let pathSection = extractPathText(entry, relativeTo);
    const diffIndicator = <DiffIndicationIcon entry={entry}/>;

    const rowActions = []
    if (onClickExpandDiff) {
        rowActions.push(new RowAction(<FileDiffIcon/>, diffExpanded ? "Hide changes" : "Show changes", diffExpanded, onClickExpandDiff))
    }
    if (onRevert) {
        rowActions.push(new RowAction(<HistoryIcon/>, "Revert changes", false, () => {
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
    let diffIndicator = <DiffIndicationIcon entry={entry}/>;
    const [showSummary, setShowSummary] = useState(false);
    if (entry.path_type === "common_prefix") {
        pathSection = <Link href={onNavigate(entry)}>{pathSection}</Link>
    }
    const rowActions = []
    rowActions.push(new RowAction(<GraphIcon/>, showSummary ? "Hide summary" : "Calculate change summary", showSummary, () => setShowSummary(!showSummary)))
    if (onRevert) {
        rowActions.push(new RowAction(<HistoryIcon/>, "Revert changes", false, () => {
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
    const diffIndicator = <DiffIndicationIcon entry={entry} isTableRow={true}/>

    const rowActions = []
    rowActions.push(new RowAction(<DiffIcon/>, "Show table changes", true, onClickExpandDiff))
    if (onRevert) {
        rowActions.push(new RowAction(<HistoryIcon/>, "Revert changes", false, () => {
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
            <td className="pl-4 col-auto p-2">{diffIndicator}</td>
            <td className="col-9 tree-path">
                        <span style={{marginLeft: (depth * 20) + "px"}}>
                            {onExpand && <PrefixExpansionSection dirExpanded={dirExpanded} onClick={onExpand}/>}
                            {loading ? <ClockIcon/> : ""}
                            {pathSection}
                        </span>
            </td>
            <td className={"col-2 p-0 text-right"}>{showSummary && <ChangeSummary prefix={entry.path} getMore={getMore}/>}</td>
            <td className={"col-1 change-entry-row-actions"}>
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

export const DiffIndicationIcon = ({entry, isTableRow = false}) => {
    let diffIcon;
    let tooltipId;
    let tooltipText;
    if (entry.path_type === 'common_prefix') {
        diffIcon = <FileDirectoryIcon/>;
        tooltipId = "tooltip-prefix";
        tooltipText = "Changes under prefix";
    }
    if (isTableRow) {
        diffIcon = <TableIcon/>;
        tooltipId = "tooltip-table";
        tooltipText = "Table changed"
    }

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

    return <OverlayTrigger placement="bottom" overlay={(<Tooltip id={tooltipId}>{tooltipText}</Tooltip>)}>
                <span>
                    {diffIcon}
                </span>
    </OverlayTrigger>;
}
