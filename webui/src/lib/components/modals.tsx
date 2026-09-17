import React, { FC, useState, ReactNode, MouseEventHandler } from 'react';

import Modal from 'react-bootstrap/Modal';
import Button from 'react-bootstrap/Button';
import Tooltip from 'react-bootstrap/Tooltip';
import { OverlayTrigger } from 'react-bootstrap';
import { ButtonVariant } from 'react-bootstrap/esm/types';

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
    modalVariant?: ButtonVariant;
    size?: 'sm' | 'lg';
    disabled?: boolean;
    tooltip?: ReactNode;
    children: ReactNode;
}

export const ConfirmationModal: FC<ConfirmationModalProps> = ({ show, onHide, msg, onConfirm, variant = 'danger' }) => {
    return (
        <Modal show={show} onHide={onHide}>
            <Modal.Header>
                <Modal.Title>Confirmation</Modal.Title>
            </Modal.Header>
            <Modal.Body>{msg}</Modal.Body>
            <Modal.Footer>
                <Button variant={variant} onClick={onConfirm}>
                    Yes
                </Button>
                <Button variant="secondary" onClick={onHide}>
                    Cancel
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

export const ConfirmationButton: FC<ConfirmationButtonProps> = ({
    msg,
    onConfirm,
    variant,
    modalVariant = 'danger',
    size,
    disabled = false,
    tooltip = null,
    children,
}) => {
    const [show, setShow] = useState(false);
    let btn = (
        <Button variant={variant} size={size} disabled={disabled} onClick={() => setShow(true)}>
            {children}
        </Button>
    );
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
                    onConfirm(hide);
                }}
                onHide={hide}
                msg={msg}
            />
            {btn}
        </>
    );
};
