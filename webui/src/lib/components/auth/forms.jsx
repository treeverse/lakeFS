import React, {useEffect, useRef, useState} from "react";
import Modal from "react-bootstrap/Modal";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import {FormControl, InputGroup} from "react-bootstrap";
import {SearchIcon} from "@primer/octicons-react";

import {useAPI} from "../../hooks/api";
import {Checkbox, DataTable, DebouncedFormControl, Error, Loading} from "../controls";

const isEmptyString = (str) => (!str?.length);


export const AttachModal = ({ show, searchFn, onAttach, onHide, addText = "Add",
                          emptyState = 'No matches', modalTitle = 'Add',
                     filterPlaceholder = 'Filter...'}) => {
    const search = useRef(null);
    const [searchPrefix, setSearchPrefix] = useState("");
    const [selected, setSelected] = useState([]);

    useEffect(() => {
        if (!!search.current && search.current.value === "")
            search.current.focus();
    });

    const { response, error, loading } = useAPI(() => {
        return searchFn(searchPrefix);
    }, [searchPrefix]);

    let content;
    if (loading) content = <Loading/>;
    else if (error) content = <Error error={error}/>;
    else content = (
            <>
                <DataTable
                    headers={['Select', 'ID']}
                    keyFn={ent => ent.id}
                    emptyState={emptyState}
                    results={response}
                    rowFn={ent => [
                        <Checkbox
                            defaultChecked={selected.indexOf(ent.id) >= 0}
                            onAdd={() => setSelected([...selected, ent.id])}
                            onRemove={() => setSelected(selected.filter(id => id !== ent.id))}
                            name={'selected'}/>,
                        <strong>{!isEmptyString(ent.email) ? ent.email : ent.id}</strong>
                    ]}/>

                <div className="mt-3">
                    {(selected.length > 0) &&
                    <p>
                        <strong>Selected: </strong>
                        {(selected.map(item => (
                            <Badge key={item} pill variant="primary" className="me-1">
                                {item}
                            </Badge>
                        )))}
                    </p>
                    }
                </div>
            </>
        );

    return (
        <Modal show={show} onHide={onHide} scrollable={true}>
            <Modal.Header closeButton>
                <Modal.Title>{modalTitle}</Modal.Title>
            </Modal.Header>
            <Modal.Body>
                <Form onSubmit={e => { e.preventDefault() }}>
                    <InputGroup>
                        <InputGroup.Text>
                            <SearchIcon/>
                        </InputGroup.Text>
                        <DebouncedFormControl
                            ref={search}
                            placeholder={filterPlaceholder}
                            onChange={() => {setSearchPrefix(search.current.value)}}/>
                    </InputGroup>
                </Form>
                <div className="mt-2">
                    {content}
                </div>
            </Modal.Body>
            <Modal.Footer>
                <Button variant="success" disabled={selected.length === 0} onClick={() => {onAttach(selected)}}>
                    {addText}
                </Button>
                <Button variant="secondary" onClick={onHide}>Cancel</Button>
            </Modal.Footer>
        </Modal>
    );
};

export const EntityActionModal = ({ show, onHide, onAction, title, placeholder, actionName, validationFunction = null }) => {
    const [error, setError] = useState(null);
    const idField = useRef(null);

    useEffect(() => {
        if (!!idField.current && idField.current.value === "")
            idField.current.focus();
    });

    const onSubmit = () => {
        if (validationFunction) {
            const validationResult = validationFunction(idField.current.value);
            if (!validationResult.isValid) {
                setError(validationResult.errorMessage);
                return;
            }
        }
        onAction(idField.current.value).catch(err => setError(err));
    };

    return (
        <Modal show={show} onHide={onHide}>
            <Modal.Header closeButton>
                <Modal.Title>{title}</Modal.Title>
            </Modal.Header>

            <Modal.Body>
                <Form onSubmit={e => {
                    e.preventDefault()
                    onSubmit()
                }}>
                    <FormControl ref={idField} autoFocus placeholder={placeholder} type="text"/>
                </Form>

                {(!!error) && <Error className="mt-3" error={error}/>}

            </Modal.Body>

            <Modal.Footer>
                <Button onClick={onSubmit} variant="success">{actionName}</Button>
                <Button onClick={onHide} variant="secondary">Cancel</Button>
            </Modal.Footer>
        </Modal>
    );
};
