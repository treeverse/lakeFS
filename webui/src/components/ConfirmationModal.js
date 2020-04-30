import Modal from "react-bootstrap/Modal";
import Button from "react-bootstrap/Button";
import React from "react";

export default ({ show, onHide, msg, onConfirm }) => {
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
};
