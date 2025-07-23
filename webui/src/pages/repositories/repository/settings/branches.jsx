import React, {useEffect, useRef, useState} from "react";
import { useOutletContext } from "react-router-dom";
import {AlertError, Loading, RefreshButton} from "../../../../lib/components/controls";
import {useRefs} from "../../../../lib/hooks/repo";
import {Button, ListGroup, Row, Badge} from "react-bootstrap";
import Col from "react-bootstrap/Col";
import {useAPI} from "../../../../lib/hooks/api";
import {branchProtectionRules} from "../../../../lib/api";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import Alert from "react-bootstrap/Alert";

const BranchProtectionRulesList = ({ rulesResponse, deleteButtonDisabled, onDeleteRule }) => {
    if (!rulesResponse) return null;
    
    const getActionBadgeVariant = (action) => {
        switch (action) {
            case 'delete':
                return 'danger';
            case 'commit':
                return 'warning';
            case 'staging_write':
                return 'info';
            default:
                return 'secondary';
        }
    };

    const getActionDisplayName = (action) => {
        switch (action) {
            case 'staging_write':
                return 'Staging Writes';
            case 'commit':
                return 'Commits';
            case 'delete':
                return 'Branch Deletion';
            default:
                return action;
        }
    };
    
    return (
        <div className="row mt-3 ms-1 pr-5">
            <ListGroup>
                {rulesResponse['rules'].length > 0 ? rulesResponse['rules'].map((r) => {
                    const blockedActions = r.blocked_actions || ['staging_write', 'commit']; // Default if not specified
                    return <ListGroup.Item key={r.pattern}>
                        <div className="d-flex flex-column">
                            <div className="d-flex align-items-center justify-content-between">
                                <div className="d-flex flex-column">
                                    <div className="mb-2">
                                        <strong>Pattern: </strong>
                                        <code className="bg-light px-2 py-1 rounded">{r.pattern}</code>
                                    </div>
                                    <div className="d-flex flex-wrap align-items-center">
                                        <small className="text-muted me-2">Blocked actions:</small>
                                        {blockedActions.map((action, index) => (
                                            <Badge 
                                                key={action} 
                                                bg={getActionBadgeVariant(action)}
                                                className={`me-1 ${index < blockedActions.length - 1 ? 'mb-1' : ''}`}
                                            >
                                                {getActionDisplayName(action)}
                                            </Badge>
                                        ))}
                                    </div>
                                </div>
                                <Button 
                                    disabled={deleteButtonDisabled} 
                                    className="ms-auto" 
                                    size="sm" 
                                    variant="outline-danger" 
                                    onClick={() => onDeleteRule(r.pattern)}
                                >
                                    Delete
                                </Button>
                            </div>
                        </div>
                    </ListGroup.Item>
                }) : <Alert variant="info">There aren&apos;t any rules yet.</Alert>}
            </ListGroup>
        </div>
    );
};

const SettingsContainer = () => {
    const {repo, loading, error} = useRefs();
    const [showCreateModal, setShowCreateModal] = useState(false);
    const [refresh, setRefresh] = useState(false);
    const [actionError, setActionError] = useState(null);
    const [deleteButtonDisabled, setDeleteButtonDisabled] = useState(false)

    const {response: rulesResponse, error: rulesError, loading: rulesLoading} = useAPI(async () => {
        return branchProtectionRules.getRules(repo.id)
    }, [repo, refresh])
    const deleteRule = (pattern) => {
        let updatedRules = [...rulesResponse['rules']]
        let lastKnownChecksum = rulesResponse['checksum']
        updatedRules = updatedRules.filter(r => r.pattern !== pattern)
        branchProtectionRules.setRules(repo.id, updatedRules, lastKnownChecksum).then(() => {
            setRefresh(!refresh)
            setDeleteButtonDisabled(false)
        }).catch(err => {
            setDeleteButtonDisabled(false)
            setActionError(err)
        })
    }
    if (error) return <AlertError error={error}/>;
    if (rulesError) return <AlertError error={rulesError}/>;
    if (actionError) return <AlertError error={actionError}/>;
    return (<>
        <div className="mt-3 mb-5">
            <div className="section-title">
                <h4 className="mb-0">
                    <div className="ms-1 me-1 pl-0 d-flex">
                        <div className="flex-grow-1">Branch protection rules</div>
                        <RefreshButton className="ms-1" onClick={() => {setRefresh(!refresh)}}/>
                        <Button className="ms-2" onClick={() => setShowCreateModal(true)}>Add</Button>
                    </div>
                </h4>
            </div>
            <div>
                Define branch protection rules to prevent direct changes.&nbsp;
                Changes to protected branches can only be done by merging from other branches.&nbsp;
                {/* eslint-disable-next-line react/jsx-no-target-blank */}
                <a href="https://docs.lakefs.io/reference/protected_branches.html" target="_blank">Learn more.</a>
            </div>
            <div className="mt-3 ms-1 pr-5">
                {loading || rulesLoading ? <Loading/> :
                    <BranchProtectionRulesList 
                        rulesResponse={rulesResponse}
                        deleteButtonDisabled={deleteButtonDisabled}
                        onDeleteRule={deleteRule}
                    />}
            </div>
        </div>
        <CreateRuleModal show={showCreateModal} hideFn={() => setShowCreateModal(false)} currentRulesResponse={rulesResponse} onSuccess={() => {
            setRefresh(!refresh)
            setShowCreateModal(false)
        }} repoID={repo.id}/>
    </>);
}
const CreateRuleModal = ({show, hideFn, onSuccess, repoID, currentRulesResponse}) => {
    const [error, setError] = useState(null);
    const [createButtonDisabled, setCreateButtonDisabled] = useState(true);
    const [blockedActions, setBlockedActions] = useState({
        staging_write: true,
        commit: true,
        delete: false
    });
    const patternField = useRef(null);

    const createRule = (pattern) => {
        if (createButtonDisabled) {
            return
        }
        setError(null)
        setCreateButtonDisabled(true)
        let updatedRules = [...currentRulesResponse['rules']]
        let lastKnownChecksum = currentRulesResponse['checksum']
        
        // Build the blocked actions array from the selected checkboxes
        const selectedActions = Object.keys(blockedActions).filter(action => blockedActions[action]);
        const rule = { pattern };
        if (selectedActions.length > 0) {
            rule.blocked_actions = selectedActions;
        }
        
        updatedRules.push(rule)
        branchProtectionRules.setRules(repoID, updatedRules, lastKnownChecksum).then(onSuccess).catch(err => {
            setError(err)
            setCreateButtonDisabled(false)
        })
    }

    const handleActionChange = (action) => {
        setBlockedActions(prev => ({
            ...prev,
            [action]: !prev[action]
        }));
    };
    return <Modal show={show} onHide={() => {
        setCreateButtonDisabled(true)
        setError(null)
        hideFn()
    }}>
        <Modal.Header closeButton>
            <Modal.Title>Create Branch Protection Rule</Modal.Title>
        </Modal.Header>

        <Modal.Body className="w-100">
            <Form onSubmit={(e) => {
                e.preventDefault();
                createRule(patternField.current.value);
            }}>
                <Form.Group as={Row} controlId="pattern">
                    <Form.Label column sm={4}>Branch name pattern</Form.Label>
                    <Col>
                        <Form.Control sm={8} type="text" autoFocus ref={patternField}
                                      onChange={() => setCreateButtonDisabled(!patternField.current || !patternField.current.value)}/>
                    </Col>
                </Form.Group>
                
                <Form.Group as={Row} controlId="blocked-actions">
                    <Form.Label column sm={4}>Block actions</Form.Label>
                    <Col>
                        <Form.Check
                            type="checkbox"
                            id="staging-write-check"
                            label="Block staging area writes (upload, delete objects)"
                            checked={blockedActions.staging_write}
                            onChange={() => handleActionChange('staging_write')}
                        />
                        <Form.Check
                            type="checkbox"
                            id="commit-check"
                            label="Block commits"
                            checked={blockedActions.commit}
                            onChange={() => handleActionChange('commit')}
                        />
                        <Form.Check
                            type="checkbox"
                            id="delete-check"
                            label="Block branch deletion"
                            checked={blockedActions.delete}
                            onChange={() => handleActionChange('delete')}
                        />
                    </Col>
                </Form.Group>
            </Form>
            {error && <AlertError error={error}/>}
        </Modal.Body>
        <Modal.Footer>
            <Button disabled={createButtonDisabled} onClick={() => createRule(patternField.current.value)}
                    variant="success">Create</Button>
            <Button onClick={() => {
                setCreateButtonDisabled(true)
                setError(null)
                hideFn()
            }} variant="secondary">Cancel</Button>
        </Modal.Footer>
    </Modal>
}

const RepositorySettingsBranchesPage = () => {
  const [setActiveTab] = useOutletContext();
  useEffect(() => setActiveTab("branches"), [setActiveTab]);
  return <SettingsContainer />;
};

export default RepositorySettingsBranchesPage;
