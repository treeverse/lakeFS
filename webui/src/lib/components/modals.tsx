import React, {FC, useMemo, useState, ReactNode, MouseEventHandler, useContext} from "react";

import Modal from "react-bootstrap/Modal";
import Button from "react-bootstrap/Button";
import Tooltip from "react-bootstrap/Tooltip";
import {OverlayTrigger} from "react-bootstrap";
import { ButtonVariant } from "react-bootstrap/esm/types";
import { GetUserEmailByIdContext } from "../../pages/auth/users";
import Container from "react-bootstrap/Container";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

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

interface BasicModal {
    display: boolean;

    children: ReactNode;
    onCancel: () => void;
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

const ModalContainer = ({children, show = false, heading, onCancel, footer=null}) => {
    return (
        <Container fluid={true} className="justify-content-center">
            <Modal show={show} onHide={onCancel}
                   size="lg"
                   restoreFocus={false}
                   aria-labelledby="contained-modal-title-vcenter"
                   centered>
                <Row>
                    <Col>
                        <Modal.Header className="justify-content-center">
                            <Modal.Title>{heading}</Modal.Title>
                        </Modal.Header>
                    </Col>
                </Row>
                <Row>
                    <Col>
                        <Modal.Body className="justify-content-center">
                            {children}
                        </Modal.Body>
                    </Col>
                </Row>
                {footer &&
                    <Row>
                        <Col>
                            <Modal.Footer>
                                {footer}
                            </Modal.Footer>
                        </Col>
                    </Row>
                }
            </Modal>
        </Container>
    )
};

export const ComingSoonModal: FC<BasicModal> = ({display, children, onCancel}) => {
    return (
        <ModalContainer show={display} heading={"Coming soon!"} onCancel={onCancel}>
            {children}
        </ModalContainer>
    )
};
