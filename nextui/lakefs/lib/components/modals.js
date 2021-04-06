import Modal from "react-bootstrap/Modal";
import Button from "react-bootstrap/Button";
import React, {useState} from "react";
import Tooltip from "react-bootstrap/Tooltip";
import {OverlayTrigger} from "react-bootstrap";

export const ConfirmationModal = ({ show, onHide, msg, onConfirm }) => {
    return (
        <Modal show={show} onHide={onHide}>
            <Modal.Header>
                <Modal.Title>Confirmation</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                {msg}
            </Modal.Body>
            <Modal.Footer>
                <Button variant="danger" onClick={onConfirm}>Yes</Button>
                <Button variant="secondary" onClick={onHide}>Cancel</Button>
            </Modal.Footer>
        </Modal>
    )
}


export const ConfirmationButton = ({ msg, onConfirm, variant, disabled = false, tooltip = null, children }) => {
    const [show, setShow] = useState(false)
    let btn = <Button variant={variant} disabled={disabled} onClick={() => setShow(true)}>{children}</Button>
    if (tooltip !== null) {
        btn = (
            <OverlayTrigger placement="bottom" overlay={<Tooltip>{tooltip}</Tooltip>}>
                {btn}
            </OverlayTrigger>
        )
    }

    return (
        <>
            <ConfirmationModal
                show={show}
                onConfirm={onConfirm}
                onHide={() => setShow(false)}
                msg={msg}
            />
            {btn}
        </>
    )
}
