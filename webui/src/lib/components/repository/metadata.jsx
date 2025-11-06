import React from "react";

import {PlusIcon, XIcon} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

/**
 * MetadataFields is a component that allows the user to add/remove key-value pairs of metadata.
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields - current metadata fields to display
 * @param {Function} setMetadataFields - callback to update the metadata fields
 * @param rest - any other props to pass to the component
 */
export const MetadataFields = ({ metadataFields, setMetadataFields, ...rest}) => {
    const onChangeKey = (i) => {
        return e => {
            const newKey = e.currentTarget.value;
            setMetadataFields(prev => [...prev.slice(0,i), {...prev[i], key: newKey}, ...prev.slice(i+1)]);
        };
    };

    const onChangeValue = (i) => {
        return e => {
            const newValue = e.currentTarget.value;
            setMetadataFields(prev => [...prev.slice(0,i),  {...prev[i], value: newValue}, ...prev.slice(i+1)]);
        };
    };

    const onBlurKey = (i) => () => {
        setMetadataFields(prev => [...prev.slice(0,i),  {...prev[i], touched: true}, ...prev.slice(i+1)]);
    };

    const onRemoveKeyValue = (i) => {
        return () => setMetadataFields(prev => [...prev.slice(0, i), ...prev.slice(i + 1)]);
    };

    const onAddKeyValue = () => {
        setMetadataFields(prev => [...prev, {key: "", value: "", touched: false}]);
    };

    return (
        <div className="mt-3 mb-3" {...rest}>
            {metadataFields.map((f, i) => {
                const showError = isEmptyKey(f.key) && f.touched;
                return (
                    <Form.Group key={`commit-metadata-field-${i}`} className="mb-3">
                        <Row>
                            <Col md={{span: 5}}>
                                <Form.Control
                                    type="text"
                                    placeholder="Key"
                                    value={f.key}
                                    onChange={onChangeKey(i)}
                                    onBlur={onBlurKey(i)}
                                    isInvalid={showError}
                                />
                                {showError && (
                                    <Form.Control.Feedback type="invalid">
                                        Key is required
                                    </Form.Control.Feedback>
                                )}
                            </Col>
                            <Col md={{span: 5}}>
                                <Form.Control type="text" placeholder="Value" value={f.value}  onChange={onChangeValue(i)}/>
                            </Col>
                            <Col md={{span: 1}}>
                                <Form.Text>
                                    <Button size="sm" variant="secondary" onClick={onRemoveKeyValue(i)} aria-label={`Remove metadata field ${i + 1}`} >
                                        <XIcon/>
                                    </Button>
                                </Form.Text>
                            </Col>
                        </Row>
                    </Form.Group>
                )
            })}
            <Button onClick={onAddKeyValue} size="sm" variant="secondary">
                <PlusIcon/>{' '}
                Add Metadata field
            </Button>
        </div>
    )
}

const isEmptyKey = (key) => !key || key.trim() === "";

/**
 * Validates metadata fields and marks empty keys as touched to show validation errors.
 * Use this before submitting to ensure all keys are filled in.
 *
 * @param {Array<{key: string, value: string, touched: boolean}>} metadataFields - Array of metadata field objects
 * @param {Function} setMetadataFields - Setter function to update metadata fields
 * @returns {boolean} True if validation passed (no empty keys), false if validation failed
 */
export const validateMetadataKeys = (metadataFields, setMetadataFields) => {
    const hasEmptyKeys = metadataFields.some(f => isEmptyKey(f.key));
    if (!hasEmptyKeys) return true;

    setMetadataFields(prev =>
        prev.map(f => isEmptyKey(f.key) ? {...f, touched: true} : f)
    );
    return false;
};
