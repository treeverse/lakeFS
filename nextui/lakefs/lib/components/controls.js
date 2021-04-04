import React, {useCallback, useEffect, useRef, useState} from 'react';
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";
import Button from "react-bootstrap/Button";
import {ClippyIcon} from "@primer/octicons-react";
import Tooltip from "react-bootstrap/Tooltip";
import Overlay from "react-bootstrap/Overlay";

const defaultDebounceMs = 300;

export function debounce(func, wait, immediate) {
    let timeout;
    return function() {
        let context = this, args = arguments;
        let later = function() {
            timeout = null;
            if (!immediate) func.apply(context, args);
        };
        let callNow = immediate && !timeout;
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
        if (callNow) func.apply(context, args);
    };
}

export const useDebounce = (func, wait = defaultDebounceMs) => {
    const debouncedRef = useRef(debounce(func, wait))
    return debouncedRef.current;
}

export const useDebouncedState = (dependsOn, debounceFn, wait = 300) => {
    const [state, setState] = useState(dependsOn)
    useEffect(() => setState(dependsOn), [dependsOn])
    const dfn = useDebounce(debounceFn, wait)

    return [state, newState => {
        setState(newState)
        dfn(newState)
    }]
}

// export const useDebouncedState = (initialValue, debounceOnChangeFn, wait = defaultDebounceMs) => {
//     const [value, setValue] = useState(initialValue)
//     const debouncedFn = useDebounce(debounceOnChangeFn, wait)
//
//     return [value, (newValue) => {
//         setValue(newValue)
//         debouncedFn(newValue)
//     }];
// }

export const DebouncedFormControl = React.forwardRef((props, ref) => {
    const onChange = debounce(props.onChange, (props.debounce !== undefined) ? props.debounce : defaultDebounceMs)
    return (<Form.Control ref={ref} {...{...props, onChange}}/>);
});

export const Loading = () => {
    return (
        <Alert variant={"info"}>Loading...</Alert>
    )
}

export const Error = ({error, onDismiss = null}) => {
    let msg = error.toString()
    // handle wrapped errors
    let err = error
    while (!!err.error) err = err.error
    if (!!err.message) msg = err.message
    if (onDismiss !== null) {
        return <Alert variant="danger" dismissible onClose={onDismiss}>{msg}</Alert>
    }
    return (
        <Alert variant="danger">{msg}</Alert>
    )
}

export const ActionGroup = ({ children, orientation = "left" }) => {
    return (
        <div role="toolbar" className={`float-${orientation} mb-2 btn-toolbar`}>
            {children}
        </div>
    )
}

export const ActionsBar = ({ children }) => {
    return (
        <div className="action-bar mt-3">
            {children}
        </div>
    )
}

const copyTextToClipboard = (text, onSuccess, onError) => {
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
        document.execCommand('copy');
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
    const [value, setValue] = useState(false)

    const ref = useRef(null)

    const handleMouseOver = () => setValue(true)
    const handleMouseOut = () => setValue(false)

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
    )

    return [ref, value]
}

export const ClipboardButton = ({ text, variant, onSuccess, onError, icon = <ClippyIcon/>,  tooltip = "Copy to clipboard"}) => {

    const [show, setShow] = useState(false);
    const [tooltipText, setTooltipText] = useState(tooltip);

    const [target, isHovered] = useHover();

    let updater = null;

    return (
        <>
            <Overlay placement="bottom" show={show || isHovered} target={target.current} onExited={() => { if (target.current != null) setTooltipText(tooltip) }}>
                {props => {updater = props.scheduleUpdate; props.show = undefined; return (<Tooltip {...props}>{tooltipText}</Tooltip>); }}
            </Overlay>
            <Button variant={variant} ref={target} onClick={(e) => {
                setShow(false);
                setTooltipText('Copied!');
                if (updater != null) updater();
                setShow(true);
                setTimeout(() => {
                    if (target.current != null) setShow(false);
                }, 2500);
                copyTextToClipboard(text, onSuccess, onError);
            }}>
                {icon}
            </Button>
        </>
    );
}
