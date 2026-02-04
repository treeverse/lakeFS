import React, { useState, ReactNode } from 'react';

import Form from 'react-bootstrap/Form';
import Button from 'react-bootstrap/Button';
import { Col, Row } from 'react-bootstrap';
import ListGroup from 'react-bootstrap/ListGroup';
import { GitBranchIcon, TagIcon } from '@primer/octicons-react';

import { RecentRef, RefType } from '../../hooks/useRecentRefs';
import { MAX_UNTRIMMED_RESULT_LENGTH, getRefDisplayName } from '../../utils/refDisplayName';

type SelectedRef = {
    id: string;
    type: string;
} | null;

type RecentRefSelectorProps = {
    recentRefs: RecentRef[];
    clearRecentRefs: () => void;
    selected: SelectedRef;
    selectRef: (ref: { id: string; type: RefType }) => void;
    onTrackRef: (refId: string, type: RefType) => void;
    refTypeNav: ReactNode;
};

export const RecentRefSelector = ({
    recentRefs,
    clearRecentRefs,
    selected,
    selectRef,
    onTrackRef,
    refTypeNav,
}: RecentRefSelectorProps) => {
    const [filter, setFilter] = useState('');

    const filteredRefs = filter
        ? recentRefs.filter((ref) => ref.id.toLowerCase().includes(filter.toLowerCase()))
        : recentRefs;

    // Apply prefix replacement logic for long ref names
    const replacePrefix = filteredRefs.some((ref) => ref.id.length > MAX_UNTRIMMED_RESULT_LENGTH) ? filter : undefined;

    return (
        <div className="ref-selector">
            <div className="ref-filter-form">
                <Form onSubmit={(e) => e.preventDefault()}>
                    <Form.Control type="text" placeholder="Filter recent" onChange={(e) => setFilter(e.target.value)} />
                </Form>
            </div>
            {refTypeNav}
            <div className="ref-scroller">
                {filteredRefs.length > 0 ? (
                    <>
                        <ListGroup as="ul" className="ref-list">
                            {filteredRefs.map((ref) => (
                                <ListGroup.Item as="li" key={ref.id}>
                                    <Row className="align-items-center">
                                        <Col xs="auto" className="pe-0">
                                            {ref.type === 'branch' ? (
                                                <GitBranchIcon size={16} />
                                            ) : (
                                                <TagIcon size={16} />
                                            )}
                                        </Col>
                                        <Col title={ref.id} className="text-nowrap overflow-hidden text-truncate">
                                            {selected && ref.id === selected.id ? (
                                                <strong>{getRefDisplayName(ref.id, replacePrefix)}</strong>
                                            ) : (
                                                <Button
                                                    variant="link"
                                                    className="text-start text-truncate w-100 d-block"
                                                    onClick={() => {
                                                        onTrackRef(ref.id, ref.type);
                                                        selectRef({ id: ref.id, type: ref.type });
                                                    }}
                                                >
                                                    {getRefDisplayName(ref.id, replacePrefix)}
                                                </Button>
                                            )}
                                        </Col>
                                    </Row>
                                </ListGroup.Item>
                            ))}
                        </ListGroup>
                        <p className="ref-paginator">
                            <Button variant="link" size="sm" onClick={clearRecentRefs}>
                                Clear
                            </Button>
                        </p>
                    </>
                ) : (
                    <p className="text-center mt-3">
                        <small>{filter ? 'No matching recent refs' : 'No recent branches or tags'}</small>
                    </p>
                )}
            </div>
        </div>
    );
};
