import React, {useCallback, useEffect, useRef, useState} from 'react';
import dayjs from "dayjs";

import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import Tooltip from "react-bootstrap/Tooltip";
import Overlay from "react-bootstrap/Overlay";
import Table from "react-bootstrap/Table";
import {OverlayTrigger} from "react-bootstrap";
import {CheckIcon, PasteIcon, SearchIcon, SyncIcon, AlertIcon, AlertFillIcon} from "@primer/octicons-react";
import {Link} from "./nav";
import {
    Box,
    Button as MuiButton,
    CircularProgress,
    Dialog,
    DialogActions,
    DialogContent,
    DialogContentText,
    DialogTitle,
    Typography
} from "@mui/material";
import InputGroup from "react-bootstrap/InputGroup";


const defaultDebounceMs = 300;

export const debounce = (func, wait, immediate) => {
    let timeout;
    return function() {
        let args = arguments;
        let later = function() {
            timeout = null;
            if (!immediate) func.apply(null, args);
        };
        let callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func.apply(null, args);
    };
}

export const useDebounce = (func, wait = defaultDebounceMs) => {
    const debouncedRef = useRef(debounce(func, wait))
    return debouncedRef.current;
};

export const useDebouncedState = (dependsOn, debounceFn, wait = 300) => {
    const [state, setState] = useState(dependsOn);
    useEffect(() => setState(dependsOn), [dependsOn]);
    const dfn = useDebounce(debounceFn, wait);

    return [state, newState => {
        setState(newState)
        dfn(newState)
    }];
}

export const DebouncedFormControl = React.forwardRef((props, ref) => {
    const onChange = debounce(props.onChange, (props.debounce !== undefined) ? props.debounce : defaultDebounceMs)
    return (<Form.Control ref={ref} {...{...props, onChange}}/>);
});
DebouncedFormControl.displayName = "DebouncedFormControl";


export const Spinner = () => {
    return (
        <div className="loading-spinner mb-3">
            <div className="spinner-border text-primary" role="status" style={{ width: '3rem', height: '3rem' }}>
                <span className="visually-hidden">Loading...</span>
            </div>
        </div>
    );
};

export const Loading = ({message = "Loading..."}) => {
    return (
        <div className="loading-container d-flex flex-column align-items-center justify-content-center py-5">
            <Spinner />
            <div className="loading-text text-center text-muted">
                {message}
            </div>
        </div>
    );
};

export const Na = () => {
    return (
        <span>&mdash;</span>
    );
};

export const AlertError = ({error, onDismiss = null, className = null}) => {
    let content = React.isValidElement(error) ? error : error.toString();
    // handle wrapped errors
    let err = error;
    while (err.error) err = err.error;
    if (err.message) content = err.message;

    const alertClassName = `${className} text-wrap text-break shadow-sm`.trim();

    return (
        <Alert 
            className={alertClassName} 
            variant="danger" 
            dismissible={onDismiss !== null}
            onClose={onDismiss}
        >
            <div className="alert-error-body">
                <div className="me-3">
                    <AlertFillIcon size={24} />
                </div>
                <div>{content}</div>
            </div>
        </Alert>
    );    
};

export const FormattedDate = ({ dateValue, format = "MM/DD/YYYY HH:mm:ss" }) => {
    if (typeof dateValue === 'number') {
        return (
            <span>{dayjs.unix(dateValue).format(format)}</span>
        );
    }

    return (
        <OverlayTrigger placement="bottom" overlay={<Tooltip>{dateValue}</Tooltip>}>
            <span>{dayjs(dateValue).format(format)}</span>
        </OverlayTrigger>
    );
};


export const ActionGroup = ({ children, orientation = "left", className = "" }) => {
    const side = (orientation === 'right') ? 'ms-auto' : '';
    return (
        <div role="toolbar" className={`${side} mb-2 btn-toolbar action-group-${orientation} ${className}`}>
            {children}
        </div>
    );
};

export const ActionsBar = ({ children }) => {
    return (
        <div className="action-bar d-flex mb-3">
            {children}
        </div>
    );
};

export const copyTextToClipboard = async (text, onSuccess, onError) => {
    const textArea = document.createElement('textarea');

    //
    // *** This styling is an extra step which is likely not required. ***
    //
    // Why is it here? To ensure:
    // 1. the element is able to have focus and selection.
    // 2. if element was to flash render it has minimal visual impact.
    // 3. less flakyness with selection and copying which **might** occur if
    //    the textarea element is not visible.
    //
    // The likelihood is the element won't even render, not even a
    // flash, so some of these are just precautions. However in
    // Internet Explorer the element is visible whilst the popup
    // box asking the user for permission for the web page to
    // copy to the clipboard.
    //

    // Place in top-left corner of screen regardless of scroll position.
    textArea.style.position = 'fixed';
    textArea.style.top = 0;
    textArea.style.left = 0;

    // Ensure it has a small width and height. Setting to 1px / 1em
    // doesn't work as this gives a negative w/h on some browsers.
    textArea.style.width = '2em';
    textArea.style.height = '2em';

    // We don't need padding, reducing the size if it does flash render.
    textArea.style.padding = 0;

    // Clean up any borders.
    textArea.style.border = 'none';
    textArea.style.outline = 'none';
    textArea.style.boxShadow = 'none';

    // Avoid flash of white box if rendered for any reason.
    textArea.style.background = 'transparent';


    textArea.value = text;

    document.body.appendChild(textArea);
    textArea.focus();
    textArea.select();

    let err = null;
    try {
        if ('clipboard' in navigator) {
            await navigator.clipboard.writeText(text);
        } else {
            document.execCommand('copy', true, text);
        }
    } catch (e) {
        err = e;
    }

    if (!!onSuccess && err === null) {
        onSuccess();
    }
    if (!!onError && err !== null) {
        onError(err);
    }

    document.body.removeChild(textArea);
};

export const useHover = () => {
    const [value, setValue] = useState(false);

    const ref = useRef(null);

    const handleMouseOver = () => setValue(true);
    const handleMouseOut = () => setValue(false);

    useEffect(
        () => {
            const node = ref.current;
            if (node) {
                node.addEventListener('mouseover', handleMouseOver);
                node.addEventListener('mouseout', handleMouseOut);

                return () => {
                    node.removeEventListener('mouseover', handleMouseOver);
                    node.removeEventListener('mouseout', handleMouseOut);
                };
            }
        },
        [ref] // Recall only if ref changes
    );

    return [ref, value];
};

export const LinkButton = ({ href, children, buttonVariant, tooltip = null }) => {
    if (tooltip === null) {
        return <Link href={href} component={Button} variant={buttonVariant}>{children}</Link>
    }
    return (
        <Link href={href} component={TooltipButton} tooltip={tooltip} variant={buttonVariant}>{children}</Link>
    );
};

export const TooltipButton = ({ onClick, variant, children, tooltip, className="", size = "sm" }) => {
    return (
        <OverlayTrigger placement="bottom" overlay={<Tooltip>{tooltip}</Tooltip>}>
            <Button variant={variant} onClick={onClick} className={className} size={size}>
                {children}
            </Button>
        </OverlayTrigger>
    );
};

export const ClipboardButton = ({ text, variant, onSuccess, icon = <PasteIcon/>, onError, tooltip = "Copy to clipboard", ...rest}) => {

    const [show, setShow] = useState(false);
    const [copied, setCopied] = useState(false);
    const [target, isHovered] = useHover();

    const currentIcon = (!copied) ? icon : <CheckIcon/>;

    let updater = null;

    return (
        <>
            <Overlay
                placement="bottom"
                show={show || isHovered}
                target={target.current}>
                {props => {
                    updater = props.popper && props.popper.scheduleUpdate;
                    props.show = undefined
                    return (<Tooltip {...props}>{tooltip}</Tooltip>)
                }}
            </Overlay>
            <Button variant={variant} ref={target} onClick={() => {
                setShow(false)
                setCopied(true)
                if (updater !== null) updater()
                setTimeout(() => {
                    if (target.current !== null) setCopied(false)
                }, 1000);
                copyTextToClipboard(text, onSuccess, onError);
            }} {...rest}>
                {currentIcon}
            </Button>
        </>
    );
};

export const PrefixSearchWidget = ({ onFilter, text = "Search by Prefix", defaultValue = "" }) => {

    const [expanded, setExpanded] = useState(!!defaultValue)

    const toggle = useCallback((e) => {
        e.preventDefault()
        setExpanded((prev) => {
            return !prev
        })
    }, [setExpanded])

    const ref = useRef(null);

    const handleSubmit = useCallback((e) => {
        e.preventDefault()
        onFilter(ref.current.value)
    }, [ref])

    if (expanded) {
        return (
            <Form onSubmit={handleSubmit} className="prefix-search-form">
                <InputGroup className="prefix-search-input-group">
                    <Form.Control
                        ref={ref}
                        autoFocus
                        defaultValue={defaultValue}
                        placeholder={text}
                        aria-label={text}
                        className="prefix-search-expanded"
                    />
                    <Button variant="light" onClick={toggle}>
                        <SearchIcon/>
                    </Button>
                </InputGroup>
            </Form>
        )
    }

    return (
        <OverlayTrigger placement="bottom" overlay={
            <Tooltip>
                {text}
            </Tooltip>
        }>
            <Button variant="light" onClick={toggle}>
                <SearchIcon/>
            </Button>
        </OverlayTrigger>
    )
}

export const RefreshButton = ({ onClick, size = "md", variant = "light", tooltip = "Refresh", icon = <SyncIcon/> }) => {
    return (
        <TooltipButton
            tooltip={tooltip}
            variant={variant}
            onClick={onClick}
            size={size}>
            {icon}
        </TooltipButton>
    );
};

export const DataTable = ({ headers, results, rowFn, keyFn = (row) => row[0], actions = [],
                              emptyState = null, firstFixedCol = false }) => {

    if ((!results || results.length === 0) && emptyState !== null) {
        return <Alert variant="warning">{emptyState}</Alert>;
    }

    return (
        <Table className="w-100" style={{ tableLayout: "fixed" }}>
            <thead>
                <tr>
                {headers.map((header, i) => (
                    <th
                        key={header}
                        title={header}
                        style={firstFixedCol && i === 0 ? { width: "30px" } : {}}
                        className="text-nowrap overflow-hidden text-truncate align-middle"
                    >
                        {header}
                    </th>
                ))}
                {(!!actions && actions.length > 0) && <th/>}
                </tr>
            </thead>
            <tbody>
            {results.map(row => (
                <tr key={keyFn(row)}>
                    {rowFn(row).map((cell, i) => (
                        <td
                            key={`${keyFn(row)}-${i}`}
                            title={keyFn(row)}
                            className="text-nowrap overflow-hidden text-truncate align-middle"
                        >
                            {cell}
                        </td>
                    ))}
                    {(!!actions && actions.length > 0) && (
                        <td>
                            <span className="row-hover">
                                {actions.map(action => (
                                    <span key={`${keyFn(row)}-${action.key}`}>
                                        {action.buttonFn(row)}
                                    </span>
                                ))}
                             </span>
                        </td>
                    )}
                </tr>
            ))}
            </tbody>
        </Table>
    );
};

export const Checkbox = ({ name, onAdd, onRemove, disabled = false, defaultChecked = false }) => {
    return (
        <Form.Group>
            <Form.Check defaultChecked={defaultChecked} disabled={disabled} type="checkbox" name={name} onChange={(e) => {
                if (e.currentTarget.checked) {
                    onAdd(name)
                } else {
                    onRemove(name)
                }
            }}/>
        </Form.Group>
    );
};

export const ToggleSwitch = ({  label, id, defaultChecked, onChange }) => {
    return (
        <Form>
            <Form.Switch
                label={label}
                id={id}
                defaultChecked={defaultChecked}
                onChange={(e) => onChange(e.target.checked)}
            />
        </Form>
    )
};

export const Warning = (props) =>
<>
    <Alert variant="warning" className="shadow-sm">
        <div className="d-flex align-items-center">
            <div className="me-3">
                <AlertIcon size={24} />
            </div>
            <div>{ props.children }</div>
        </div>
    </Alert>
</>;

export const Warnings = ({ warnings = [] }) => {
    return <ul className="pl-0 ms-0 warnings">
           {warnings.map((warning, i) =>
           <Warning key={i}>{warning}</Warning>
           )}
       </ul>;
};

export const ProgressSpinner = ({text, changingElement =''}) => {
    return (
        <Box sx={{display: 'flex', alignItems: 'center'}}>
            <Box>
                <CircularProgress size={50}/>
            </Box>
            <Box sx={{p: 4}}>
                <Typography>{text}{changingElement}</Typography>
            </Box>
        </Box>
    );
}

export const ExitConfirmationDialog = ({dialogAlert, dialogDescription, onExit, onContinue, isOpen=false}) => {
    return (
        <Dialog
            open={isOpen}
            aria-labelledby="alert-dialog-title"
            aria-describedby="alert-dialog-description"
        >
            <DialogTitle id="alert-dialog-title">
                {dialogAlert}
            </DialogTitle>
            <DialogContent>
                <DialogContentText id="alert-dialog-description">
                    {dialogDescription}
                </DialogContentText>
            </DialogContent>
            <DialogActions>
                <MuiButton onClick={onContinue} autoFocus>Cancel</MuiButton>
                <MuiButton onClick={onExit}>
                    Exit
                </MuiButton>
            </DialogActions>
        </Dialog>
    );
};


export const ExperimentalOverlayTooltip = ({children, show = true, placement="auto"}) => {
    const experimentalTooltip = () => (
        <Tooltip id="button-tooltip" >
            Experimental
        </Tooltip>
    );
    return show ? (
        <OverlayTrigger
            placement={placement}
            overlay={experimentalTooltip()}
        >
            {children}
        </OverlayTrigger>
    ) : <></>;
};

export const GrayOut = ({children}) =>
    <div style={{position: 'relative'}}>
               <div>
                   <div className={'gray-out overlay'}/>
                   {children}
               </div>
           </div>;


export const WrapIf = ({enabled, Component, children}) => (
    enabled ? <Component>{children}</Component> : children);

export const SearchInput = ({searchPrefix, setSearchPrefix, placeholder}) => {
    return (
        <InputGroup>
            <Form.Control
                autoFocus
                placeholder={placeholder}
                value={searchPrefix}
                onChange={(e) => setSearchPrefix(e.target.value)}
            />
            <InputGroup.Text>
                <SearchIcon />
            </InputGroup.Text>
        </InputGroup>
    );
};
