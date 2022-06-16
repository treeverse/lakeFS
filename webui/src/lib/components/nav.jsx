import React from "react";
import { Link as RouterLink } from "react-router-dom";

import Nav from "react-bootstrap/Nav";

import {buildURL} from "../hooks/router";


function isModifiedEvent(event) {
    return !!(event.metaKey || event.altKey || event.ctrlKey || event.shiftKey);
}

const wrapComponent = (component) => {
    const linkWrapper = React.forwardRef(({ navigate, onClick, ...rest}, forwardedRef) => {
        const { target } = rest;
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

export const NavItem = ({ href, active, children }) => {
    return (
        <Nav.Item>
            <Link href={href} component={Nav.Link} active={active}>
                <>{children}</>
            </Link>
        </Nav.Item>
    );
};