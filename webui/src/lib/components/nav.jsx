import React from "react";
import {Link as RouterLink} from "react-router-dom";

import Nav from "react-bootstrap/Nav";

import {buildURL} from "../hooks/router";
import {Box, Tabs} from "@mui/material";
import {ClipboardButton} from "./controls";
import {FaRegCopy} from "react-icons/fa";


function isModifiedEvent(event) {
    return !!(event.metaKey || event.altKey || event.ctrlKey || event.shiftKey);
}

const wrapComponent = (component) => {
    const linkWrapper = React.forwardRef(({navigate, onClick, ...rest}, forwardedRef) => {
        const {target} = rest;
        const props = {
            ...rest,
            ref: forwardedRef,
            onClick: event => {
                try {
                    if (onClick) onClick(event);
                } catch (ex) {
                    event.preventDefault();
                    throw ex;
                }

                if (
                    !event.defaultPrevented && // onClick prevented default
                    event.button === 0 && // ignore everything but left clicks
                    (!target || target === "_self") && // let browser handle "target=_blank" etc.
                    !isModifiedEvent(event) // ignore clicks with modifier keys
                ) {
                    event.preventDefault();
                    navigate();
                }
            }
        };
        return React.createElement(component, props);
    });
    linkWrapper.displayName = "linkWrapper";
    return linkWrapper;
}

export const Link = (props) => {
    const dontPassTheseProps = ['href', 'to', 'children', 'components'];
    const linkProps = {to: (props.href) ? buildURL(props.href) : props.href};
    Object.getOwnPropertyNames(props)
        .filter(k => dontPassTheseProps.indexOf(k) === -1)
        .forEach(k => linkProps[k] = props[k]);
    if (props.component)
        linkProps.component = wrapComponent(props.component);

    return React.createElement(RouterLink, linkProps, props.children);
}

export const NavItem = ({href, active, children}) => {
    return (
        <Nav.Item>
            <Link href={href} component={Nav.Link} active={active}>
                <>{children}</>
            </Link>
        </Nav.Item>
    );
};

export function CodeTabPanel({children, value, index, ...other}) {
    return (
        <div
            role="code-tabpanel"
            hidden={value !== index}
            id={`code-tabpanel-${index}`}
            aria-labelledby={`code-tabpanel-${index}`}
            {...other}
        >
            {value === index && (
                <Box sx={{display: 'flex', flexDirection: 'row', justifyContent: 'space-between', py: 1}}
                     className={'code-container text-secondary'}>
                    <Box sx={{ml: 2}}>
                        {children}
                    </Box>
                    <Box sx={{mr: 2}}>
                        <ClipboardButton icon={<FaRegCopy size={16}/>} variant="link" tooltip="Copy to clipboard"
                                         text={children} size={'sm'}/>
                    </Box>
                </Box>
            )}
        </div>
    );
}

export const TabsWrapper = ({
                                isCentered,
                                children,
                                defaultTabIndex,
                                handleTabChange,
                                ariaLabel = '',
                                textColor = 'primary',
                                indicatorColor = 'primary'
                            }) => {
    return (
        <Tabs
            value={defaultTabIndex}
            onChange={handleTabChange}
            textColor={textColor}
            indicatorColor={indicatorColor}
            aria-label={ariaLabel}
            centered={isCentered}
        >
            {children}
        </Tabs>
    );
}
