import React, {useCallback} from "react";

import {PlusIcon, XIcon} from "@primer/octicons-react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

/**
 * MetadataFields is a component that allows the user to add/remove key-value pairs of metadata.
 * @param {Array<{key: string, value: string}>} metadataFields - initial metadata fields to display
 * @param {Function} setMetadataFields - callback to update the metadata fields
*/
export const MetadataFields = ({ metadataFields, setMetadataFields, ...rest}) => {
    const onChangeKey = (i) => {
        return e => {
            const key = e.currentTarget.value;
            setMetadataFields(prev => [...prev.slice(0,i), {...prev[i], key}, ...prev.slice(i+1)]);
        };
    };

    const onChangeValue = (i) => {
        return e => {
            const value = e.currentTarget.value;
            setMetadataFields(prev => [...prev.slice(0,i),  {...prev[i], value}, ...prev.slice(i+1)]);
        };
    };

    const onRemovePair = (i) => {
        return () => setMetadataFields(prev => [...prev.slice(0, i), ...prev.slice(i + 1)]);
    };

    const onAddPair = useCallback(() => {
        setMetadataFields(prev => [...prev, {key: "", value: ""}])
    }, [setMetadataFields]);

    return (
        <div className="mt-3 mb-3" {...rest}>
            {metadataFields.map((f, i) => {
                return (
                    <Form.Group key={`commit-metadata-field-${i}`} className="mb-3">
                        <Row>
                            <Col md={{span: 5}}>
                                <Form.Control type="text" placeholder="Key" value={f.key} onChange={onChangeKey(i)}/>
                            </Col>
                            <Col md={{span: 5}}>
                                <Form.Control type="text" placeholder="Value" value={f.value}  onChange={onChangeValue(i)}/>
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
