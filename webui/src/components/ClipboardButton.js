import React, {useEffect, useRef, useState} from "react";
import Button from "react-bootstrap/Button";
import {ClippyIcon, FilterIcon} from "@primer/octicons-react";
import Tooltip from "react-bootstrap/Tooltip";
import Overlay from "react-bootstrap/Overlay";
import {Link} from "react-router-dom";


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

function useHover() {
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
}

const ClipboardButton = ({ text, variant, onSuccess, onError, icon = <ClippyIcon/>,  tooltip = "Copy to clipboard"}) => {

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
};


const RunFilterButton = ({ variant, to, icon = <FilterIcon/>,  tooltip}) => {
    const [tooltipText, setTooltipText] = useState(tooltip);
    const [target, isHovered] = useHover();

    return (
        <>
            <Link to={to}>
            <Overlay placement="bottom" show={isHovered} target={target.current} onExited={() => { if (target.current != null) setTooltipText(tooltip) }}>
                {props => { props.show = undefined; return (<Tooltip {...props}>{tooltipText}</Tooltip>); }}
            </Overlay>
            <Button variant={variant} ref={target} >
                {icon}
            </Button>
            </Link>
        </>
    );
};

export {ClipboardButton, RunFilterButton}
export default ClipboardButton