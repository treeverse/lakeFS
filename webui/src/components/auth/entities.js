import React, {useEffect, useRef, useState} from "react";
import {Alert, Button, Table} from "react-bootstrap";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";



export const EntityCreateButton = ({ createFn, resetFn, status, variant, buttonText, modalTitle, children, onDone = null, createView = null, modalSize="md", onShow = null, createButtonText = "Create" }) => {
    const [show, setShow] = useState(false);
    const [showCreateView, setShowCreateView] = useState(false);

    const formRef = useRef(null);

    const disabled = (status.inProgress);

    const onHide = () => {
        if (disabled) return;
        setShow(false);
        setShowCreateView(false);
        resetFn();
    };

    useEffect(() => {
        if (status.done) {
            if (onDone !== null)
                onDone();

            if (createView !== null) {
                setShowCreateView(true);
            } else {
                setShow(false);
            }
            resetFn();
        }
    }, [resetFn, createView, onDone, status.done]);

    const content = (createView !== null &&  showCreateView) ? createView(status.payload) : children;
    const showCreateButton = ((createView !== null && !showCreateView) || createView === null);

    const serializeForm = () => {
        const elements = formRef.current.elements;
        const formData = {};
        for (let i = 0; i < elements.length; i++) {
            const current = elements[i];
            if ((current.type === "radio" || current.type === "checkbox") && !current.checked) {
                continue;
            }
            formData[current.name] = current.value;
        }
        return formData;
    };
    const onSubmit = () => {
        if (disabled) return;
        createFn(serializeForm());
    };
    return (
        <>
            <Button variant={variant} onClick={() => {
                if (!show && onShow !== null) onShow();
                setShow(!show)
            }} disabled={disabled}>{buttonText}</Button>
            <Modal enforceFocus={false} show={show} onHide={onHide} size={modalSize}>
                <Modal.Header closeButton>
                    <Modal.Title>{modalTitle}</Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <Form ref={formRef} onSubmit={(e) => {
                        onSubmit();
                        e.preventDefault();
                    }}>
                        {content}
                    </Form>
                    {(!!status.error) ? (<Alert variant="danger">{status.error}</Alert>) : (<span/>)}
                </Modal.Body>

                {(showCreateButton) ? (
                    <Modal.Footer>
                        <Button variant="secondary" disabled={disabled} onClick={onHide}>
                            Cancel
                        </Button>
                        <Button variant="success" disabled={disabled} onClick={onSubmit}>
                            {createButtonText}
                        </Button>
                    </Modal.Footer>
                ) : (<></>)}
            </Modal>
        </>
    );
}

export const PaginatedEntryList = ({ listFn, entities, entityToRow, entityToKey, emptyState, fields, update}) => {
    const [offsets, setOffsets] = useState([""]);
    const [currentOffset, setCurrentOffset] = useState("");

    useEffect(() => {
        listFn(currentOffset);
    },[listFn, currentOffset, update]);


    const addNextOffset = (offset) => {
        if (offsets.indexOf(offset) === -1) {
            const updated = [...offsets, offset];
            offsets.sort();
            return updated;
        }
        return offsets;
    }

    const getPreviousOffset = () => {
        const offsetIndex = offsets.indexOf(currentOffset);
        if (offsetIndex === -1 || offsetIndex === 0) {
            return null;
        }
        return offsets[offsetIndex-1];
    }

    return <EntityList
        entities={entities}
        emptyState={emptyState}
        offsets={offsets}
        fields={fields}
        entityToRow={entityToRow}
        entityToKey={entityToKey}
        onNext={(nextOffset) => {
            setOffsets(addNextOffset(nextOffset));
            setCurrentOffset(nextOffset);
        }}
        hasPrevious={getPreviousOffset() !== null}
        onPrevious={() => {
            setCurrentOffset(getPreviousOffset);
        }}
    />
}

export const EntityList = ({ entities, fields, entityToRow, entityToKey = entity => entity.id, emptyState, onNext, onPrevious, hasPrevious }) => {

    if (!entityToKey) {
        entityToKey = entity => entity.id;
    }

    if (entities.loading) {
        return <p>Loading...</p>;
    }

    if (!!entities.error) {
        return <Alert variant={"danger"}>{entities.error}</Alert>
    }

    if (entities.payload.pagination.results === 0) {
        return <Alert variant="warning">{emptyState}</Alert>
    }

    return (
        <div>
            <Table>
                <thead>
                <tr>
                    {(fields.map(field  => {
                        return <th key={field}>{field}</th>
                    }))}
                </tr>
                </thead>
                <tbody>
                {entities.payload.results.map(entity => {
                    return (
                        <tr key={entityToKey(entity)}>
                            {entityToRow(entity).map((cell, i) => {
                                return <td key={`${entityToKey(entity)}-cell-${i}`}>{cell}</td>
                            })}
                        </tr>
                    );
                })}
                </tbody>
            </Table>


            <p className="tree-paginator">
                {(hasPrevious) ? (
                    <Button variant="outline-primary" onClick={() => {onPrevious() }}>
                        &lt;&lt; Previous Page
                    </Button>
                ) : null}
                {'  '}
                {(entities.payload.pagination.has_more) ? (
                    <Button variant="outline-primary" onClick={() => {
                        onNext(entities.payload.pagination.next_offset)
                    }}>
                        Next Page &gt;&gt;
                    </Button>) : null}
            </p>


        </div>
    )
}