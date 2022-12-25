import React from "react";
import {NavLink as RouterLink, useHref, useLinkClickHandler} from "react-router-dom";

import Nav from "react-bootstrap/Nav";

import {buildURL} from "../hooks/router";
import {Tabs} from "@mui/material";

const wrapComponent = (component) => {
    const linkWrapper = React.forwardRef(({navigate, onClick, to, target, replace, state, ...rest}, ref) => {
        const href = useHref(to);
        const handleClick = useLinkClickHandler(to, {
            replace,
            state,
            target,
        });

        const props = {
            ...rest,
            ref,
            href,
            onClick: (event) => {
                if (onClick && typeof onClick === "function") {
                    onClick(event);
                }

                if (!event.defaultPrevented) {
                    handleClick(event);
                }
            },
            target,
            replace,
            navigate,
        };
        return React.createElement(component, props);
    });
    linkWrapper.displayName = "linkWrapper";
    return linkWrapper;
}

export const Link = (props) => {
    const dontPassTheseProps = ['href', 'to', 'children', 'components', 'component'];
    const filteredProps = Object.entries(props).filter(([key]) => {
        return !dontPassTheseProps.includes(key);
    });
    const linkProps = Object.fromEntries(filteredProps);
    linkProps.to = props.href ? buildURL(props.href) : props.href;
    if (props.component) {
        return React.createElement(wrapComponent(props.component), linkProps, props.children);
    }

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
