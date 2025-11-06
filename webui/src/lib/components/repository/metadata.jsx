import React, {useCallback} from "react";
import {PlusIcon, XIcon} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

/**
 * MetadataFields component for adding metadata key-value pairs.
 * Used in commit, import, and merge modals.
 */
export const MetadataFields = ({ metadataFields, setMetadataFields, ...rest}) => {
    const onChangeKey = useCallback((i) => {
        return e => {
            const key = e.currentTarget.value;
            setMetadataFields(prev => [...prev.slice(0,i), {...prev[i], key}, ...prev.slice(i+1)]);
            e.preventDefault()
        };
    }, [setMetadataFields]);

    const onChangeValue = useCallback((i) => {
        return e => {
            const value = e.currentTarget.value;
            setMetadataFields(prev => [...prev.slice(0,i),  {...prev[i], value}, ...prev.slice(i+1)]);
        };
    }, [setMetadataFields]);

    const onRemovePair = useCallback((i) => {
        return () => setMetadataFields(prev => [...prev.slice(0, i), ...prev.slice(i + 1)])
    }, [setMetadataFields])

    const onAddPair = useCallback(() => {
        setMetadataFields(prev => [...prev, {key: "", value: ""}])
    }, [setMetadataFields])

    return (
        <div className="mt-3 mb-3" {...rest}>
            {metadataFields.map((f, i) => {
                return (
                    <Form.Group key={`commit-metadata-field-${i}`} className="mb-3">
                        <Row>
                            <Col md={{span: 5}}>
                                <Form.Control type="text" placeholder="Key" defaultValue={f.key} onChange={onChangeKey(i)}/>
                            </Col>
                            <Col md={{span: 5}}>
                                <Form.Control type="text" placeholder="Value" defaultValue={f.value}  onChange={onChangeValue(i)}/>
                            </Col>
                            <Col md={{span: 1}}>
                                <Form.Text>
                                    <Button size="sm" variant="secondary" onClick={onRemovePair(i)}>
                                        <XIcon/>
                                    </Button>
                                </Form.Text>
                            </Col>
                        </Row>
                    </Form.Group>
                )
            })}
            <Button onClick={onAddPair} size="sm" variant="secondary">
                <PlusIcon/>{' '}
                Add Metadata field
            </Button>
        </div>
    )
}

/**
 * Filters out metadata fields where both key and value are empty or whitespace-only.
 * Allows fields with empty values (but non-empty keys) or empty keys (but non-empty values) matching the current CLI behavior.
 *
 * @param {Array<{key: string, value: string}>} metadataFields - Array of metadata field objects
 * @returns {Array<{key: string, value: string}>} Filtered array excluding fields where both key and value are empty
 */
export const filterEmptyMetadataFields = (metadataFields) => {
    return metadataFields.filter(field =>
        (field.key && field.key.trim() !== "") ||
        (field.value && field.value.trim() !== "")
    );
};
