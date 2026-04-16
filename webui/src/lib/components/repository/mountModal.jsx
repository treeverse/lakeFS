import React from 'react';
import Modal from 'react-bootstrap/Modal';
import Form from 'react-bootstrap/Form';
import InputGroup from 'react-bootstrap/InputGroup';
import Button from 'react-bootstrap/Button';

const MountModal = ({ show, onHide, repo, reference, path }) => {
    const mountCommand = `everest mount lakefs://${repo.id}/${reference.id}/${path}`;
    return (
        <Modal show={show} onHide={onHide} size={'lg'}>
            <Modal.Header closeButton>
                <Modal.Title>Mount Directory</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <p>
                    Mount this path as a local directory using{' '}
                    <a href="https://docs.lakefs.io/reference/mount.html" target="_blank" rel="noopener noreferrer">
                        Everest
                    </a>
                    :
                </p>
                <InputGroup>
                    <Form.Control readOnly value={mountCommand} className="font-monospace" disabled />
                </InputGroup>
                <p className="text-muted mt-3">
                    Mount is only available with{' '}
                    <a href="https://lakefs.io/enterprise/" target="_blank" rel="noopener noreferrer">
                        lakeFS Enterprise
                    </a>{' '}
                    or{' '}
                    <a href="https://lakefs.cloud/register" target="_blank" rel="noopener noreferrer">
                        lakeFS Cloud
                    </a>
                    .
                </p>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="secondary" onClick={onHide}>
                    Close
                </Button>
            </Modal.Footer>
        </Modal>
    );
};

export default MountModal;
