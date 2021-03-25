import React, {useState} from "react";
import Button from "react-bootstrap/Button";
import {FilterIcon} from "@primer/octicons-react";
import Tooltip from "react-bootstrap/Tooltip";
import Overlay from "react-bootstrap/Overlay";
import {Link} from "react-router-dom";
import {useHover} from "./ClipboardButton.js"

const FilterButton = ({ variant, to, icon = <FilterIcon/>,  tooltip}) => {
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

export default FilterButton;