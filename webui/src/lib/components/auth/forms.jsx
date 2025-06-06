import React, {useEffect, useRef, useState} from "react";
import Modal from "react-bootstrap/Modal";
import Badge from "react-bootstrap/Badge";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import {FormControl, InputGroup} from "react-bootstrap";
import {SearchIcon} from "@primer/octicons-react";

import {useAPI} from "../../hooks/api";
import {Checkbox, DataTable, DebouncedFormControl, AlertError, Loading} from "../controls";
import {Paginator} from "../pagination";


export const AttachModal = ({
                              show, searchFn, resolveEntityFn = (ent => ent.id), onAttach, onHide, addText = "Add",
                              emptyState = 'No matches', modalTitle = 'Add', headers = ['', 'ID'],
                              filterPlaceholder = 'Filter...'
                            }) => {
  const search = useRef(null);
  const [paginationParams, setPaginationParams] = useState({ prefix: "", after: "" });
  const [selected, setSelected] = useState([]);

  const { response, error, loading } = useAPI(() => {
      return searchFn(paginationParams.prefix, paginationParams.after);
  }, [paginationParams]);

  useEffect(() => {
    if (!!search.current && search.current.value === "")
      search.current.focus();
  });

  const nextPage = response?.pagination?.has_more ? response.pagination.next_offset : null;

  let content;
  if (loading) content = <Loading/>;
  else if (error) content = <AlertError error={error}/>;
  else content = (
      <>
        <DataTable
          headers={headers}
          keyFn={ent => ent.id}
          emptyState={emptyState}
          results={response.results}
          rowFn={ent => [
            <Checkbox
              defaultChecked={selected.some(selectedEnt => selectedEnt.id === ent.id)}
              onAdd={() => setSelected([...selected, ent])}
              onRemove={() => setSelected(selected.filter(selectedEnt => selectedEnt.id !== ent.id))}
              name={'selected'}/>,
            <strong>{resolveEntityFn(ent)}</strong>
          ]}
          firstFixedCol={true}
        />
        <Paginator
            after={paginationParams.after}
            nextPage={nextPage}
            onPaginate={(newAfter) => setPaginationParams(prev => ({
                ...prev,
                after: newAfter
            }))}
        />
        <div className="mt-3">
          {(selected.length > 0) &&
            <p>
              <strong>Selected: </strong>
              {(selected.map(item => (
                <Badge
                  key={item.id}
                  pill
                  variant="primary"
                  className="
                      me-1
                      d-inline-block
                      w-25
                      text-nowrap
                      overflow-hidden
                      text-truncate
                      align-middle
                  "
                  title={resolveEntityFn(item)}
                >
                  {resolveEntityFn(item)}
                </Badge>
              )))}
            </p>
          }
        </div>
      </>
    );

  const handleSearchChange = () => {
      setPaginationParams(prev => ({
          ...prev,
          prefix: search.current.value,
          after: ""
      }));
  };

  return (
    <Modal show={show} onHide={onHide}>
      <Modal.Header closeButton>
        <Modal.Title>{modalTitle}</Modal.Title>
      </Modal.Header>
      <Modal.Body>
        <Form onSubmit={e => {
          e.preventDefault()
        }}>
          <InputGroup>
            <InputGroup.Text>
              <SearchIcon/>
            </InputGroup.Text>
            <DebouncedFormControl
              ref={search}
              placeholder={filterPlaceholder}
              onChange={handleSearchChange}/>
          </InputGroup>
        </Form>
        <div className="mt-2">
          {content}
        </div>
      </Modal.Body>
      <Modal.Footer>
        <Button variant="success" disabled={selected.length === 0} onClick={() => {
          onAttach(selected)
        }}>
          {addText}
        </Button>
        <Button variant="secondary" onClick={onHide}>Cancel</Button>
      </Modal.Footer>
    </Modal>
  );
};

export const EntityActionModal = ({
                                    show,
                                    onHide,
                                    onAction,
                                    title,
                                    placeholder,
                                    actionName,
                                    validationFunction = null,
                                    showExtraField = false,
                                    extraPlaceholder = "",
                                    extraValidationFunction = null
                                  }) => {
  const [error, setError] = useState(null);
  const idField = useRef(null);
  const extraField = useRef(null);

  useEffect(() => {
    if (!!idField.current && idField.current.value === "") {
      idField.current.focus();
    }
  });

  const onSubmit = () => {
    if (validationFunction) {
      const validationResult = validationFunction(idField.current.value);
      if (!validationResult.isValid) {
        setError(validationResult.errorMessage);
        return;
      }
    }
    if (showExtraField) {
      if (extraValidationFunction) {
        const validationResult = extraValidationFunction(extraField.current.value);
        if (!validationResult.isValid) {
          setError(validationResult.errorMessage);
          return;
        }
      }
      onAction(idField.current.value, extraField.current.value).catch(err => setError(err));
    } else {
      onAction(idField.current.value).catch(err => setError(err));
    }
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
          {showExtraField &&
            <FormControl ref={extraField} placeholder={extraPlaceholder} type="text" className="mt-3"/>
          }
        </Form>

        {(!!error) && <AlertError className="mt-3" error={error}/>}

      </Modal.Body>

      <Modal.Footer>
        <Button onClick={onSubmit} variant="success">{actionName}</Button>
        <Button onClick={onHide} variant="secondary">Cancel</Button>
      </Modal.Footer>
    </Modal>
  );
};
