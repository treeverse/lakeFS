import React, {FC, useMemo, useState, ReactNode, MouseEventHandler, useContext} from "react";

import Modal from "react-bootstrap/Modal";
import Button from "react-bootstrap/Button";
import Tooltip from "react-bootstrap/Tooltip";
import {OverlayTrigger} from "react-bootstrap";
import { ButtonVariant } from "react-bootstrap/esm/types";
import { GetUserEmailByIdContext } from "../../pages/auth/users";

interface ConfirmationModalProps {
    show: boolean;
    onHide: () => void;
    msg: ReactNode;
    onConfirm: MouseEventHandler<HTMLElement>;
    variant: ButtonVariant;
}

interface ConfirmationButtonProps {
    msg: ReactNode;
    onConfirm: (hide: () => void) => void;
    variant: ButtonVariant;
    modalVariant: ButtonVariant;
    size: 'sm' | 'lg';
    disabled?: boolean;
    tooltip: ReactNode;
    children: ReactNode;
}

// Extend the ConfirmationButtonProps, but omit the msg property,
// so we can extend it for this use case
interface ConfirmationButtonWithContextProps extends Omit<ConfirmationButtonProps, "msg"> {
    msg: ReactNode | ((email: string) => ReactNode);
    userId: string;
}

export const ConfirmationModal: FC<ConfirmationModalProps> = ({ show, onHide, msg, onConfirm, variant = "danger" }) => {
    return (
        <Modal show={show} onHide={onHide}>
            <Modal.Header>
                <Modal.Title>Confirmation</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                {msg}
            </Modal.Body>
            <Modal.Footer>
                <Button variant={variant} onClick={onConfirm}>Yes</Button>
                <Button variant="secondary" onClick={onHide}>Cancel</Button>
            </Modal.Footer>
        </Modal>
    );
};

export const ConfirmationButtonWithContext: FC<ConfirmationButtonWithContextProps> = ({ userId, msg, onConfirm, variant, modalVariant, size, disabled = false, tooltip = null, children }) => {
    const getUserEmailById = useContext(GetUserEmailByIdContext);
    const email = useMemo(() => getUserEmailById(userId), [userId]);
    
    let msgNode: ReactNode;
    if (typeof msg === "function") {
        msgNode = msg(email);
    } else {
        msgNode = msg;
    }

    return (
        <ConfirmationButton
            msg={msgNode}
            onConfirm={onConfirm}
            variant={variant}
            modalVariant={modalVariant}
            size={size}
            disabled={disabled}
            tooltip={tooltip}
        >{children}</ConfirmationButton>
    );
}


export const ConfirmationButton: FC<ConfirmationButtonProps> = ({ msg, onConfirm, variant, modalVariant, size, disabled = false, tooltip = null, children }) => {
    const [show, setShow] = useState(false);
    let btn = <Button variant={variant} size={size} disabled={disabled} onClick={() => setShow(true)}>{children}</Button>;
    if (tooltip !== null) {
        btn = (
            <OverlayTrigger placement="bottom" overlay={<Tooltip id="confirmation-tooltip">{tooltip}</Tooltip>}>
                {btn}
            </OverlayTrigger>
        );
    }

    const hide = () => setShow(false);

    return (
        <>
            <ConfirmationModal
                show={show}
                variant={modalVariant}
                onConfirm={() => {
                    onConfirm(hide)
                }}
                onHide={hide}
                msg={msg}
            />
            {btn}
        </>
    );
};
