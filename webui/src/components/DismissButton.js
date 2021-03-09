import React, {useState} from "react";
import Button from "react-bootstrap/Button";
import {XIcon} from "@primer/octicons-react";
import Tooltip from "react-bootstrap/Tooltip";
import Overlay from "react-bootstrap/Overlay";
import {Link} from "react-router-dom";
import {useHover} from "./ClipboardButton.js"
import Container from "react-bootstrap/Container";

const DismissButton = ({ variant, to, icon = <XIcon/>, label,  tooltip}) => {
    const [tooltipText, setTooltipText] = useState(tooltip);
    const [target, isHovered] = useHover();

    return (
        <Container>
            {label}
            <Link to={to}>
                <Overlay placement="bottom" show={isHovered} target={target.current} onExited={() => { if (target.current != null) setTooltipText(tooltip) }}>
                    {props => { props.show = undefined; return (<Tooltip {...props}>{tooltipText}</Tooltip>); }}
                </Overlay>
                <Button size="sm" className="dismiss-btn" variant={variant} ref={target} >
                    {icon}
                </Button>
            </Link>
        </Container>
    );
};

export default DismissButton;