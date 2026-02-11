import React, { useEffect, useState } from 'react';
import { useOutletContext } from 'react-router-dom';
import { AlertError, Loading, RefreshButton } from '../../../../lib/components/controls';
import Button from 'react-bootstrap/Button';
import { repositories } from '../../../../lib/api';
import { useAPI } from '../../../../lib/hooks/api';
import { useRefs } from '../../../../lib/hooks/repo';
import Alert from 'react-bootstrap/Alert';
import { MetadataFields } from '../../../../lib/components/repository/metadata';
import { getMetadataIfValid, touchInvalidFields } from '../../../../lib/components/repository/metadataHelpers';

const metadataToFields = (metadata) => {
    if (!metadata || Object.keys(metadata).length === 0) return [];
    return Object.entries(metadata).map(([key, value]) => ({
        key,
        value,
        touched: false,
    }));
};

const RepoMetadata = ({ repo }) => {
    const [metadataFields, setMetadataFields] = useState([]);
    const [originalMetadata, setOriginalMetadata] = useState({});
    const [isSaving, setIsSaving] = useState(false);
    const [saveError, setSaveError] = useState(null);
    const [saveSuccess, setSaveSuccess] = useState(false);

    const { response, error, loading, refetch } = useAPI(async () => {
        return await repositories.getMetadata(repo.id);
    }, [repo]);

    useEffect(() => {
        if (response) {
            setMetadataFields(metadataToFields(response));
            setOriginalMetadata(response || {});
            setSaveSuccess(false);
            setSaveError(null);
        }
    }, [response]);

    const onSave = async () => {
        const metadata = getMetadataIfValid(metadataFields);
        if (metadata === null) {
            setMetadataFields(touchInvalidFields(metadataFields));
            return;
        }

        setIsSaving(true);
        setSaveError(null);
        setSaveSuccess(false);

        try {
            // Find keys that were removed
            const currentKeys = new Set(Object.keys(metadata));
            const removedKeys = Object.keys(originalMetadata).filter((k) => !currentKeys.has(k));

            // Delete removed keys first
            if (removedKeys.length > 0) {
                await repositories.deleteMetadata(repo.id, removedKeys);
            }

            // Set current metadata (adds/updates)
            if (Object.keys(metadata).length > 0) {
                await repositories.setMetadata(repo.id, metadata);
            }

            setSaveSuccess(true);
            refetch();
        } catch (err) {
            setSaveError(err);
        } finally {
            setIsSaving(false);
        }
    };

    const isReadOnly = repo?.read_only;

    let content;
    if (loading) {
        content = <Loading />;
    } else if (error) {
        content = <AlertError error={error} />;
    } else {
        const hasFields = metadataFields.length > 0;
        content = (
            <>
                {!hasFields && (
                    <Alert variant="info" className="mt-3">
                        No metadata has been set for this repository yet.
                    </Alert>
                )}
                <MetadataFields
                    metadataFields={metadataFields}
                    setMetadataFields={setMetadataFields}
                    disabled={isSaving || isReadOnly}
                />
                {saveError && <AlertError error={saveError} />}
                {saveSuccess && (
                    <Alert variant="success" className="mt-3">
                        Metadata saved successfully.
                    </Alert>
                )}
                {!isReadOnly && (
                    <Button className="mt-2" disabled={isSaving} onClick={onSave}>
                        {isSaving ? 'Saving...' : 'Save'}
                    </Button>
                )}
            </>
        );
    }

    return (
        <div className="mt-3 mb-5">
            <div className="section-title">
                <h4 className="mb-0">
                    <div className="ms-1 me-1 pl-0 d-flex">
                        <div className="flex-grow-1">Repository metadata</div>
                        <RefreshButton className="ms-1" onClick={refetch} />
                    </div>
                </h4>
            </div>
            <p className="mt-3">
                Manage key-value metadata associated with this repository.
            </p>
            <div className="mt-3">{content}</div>
        </div>
    );
};

const MetadataContainer = () => {
    const { repo, loading, error } = useRefs();
    if (loading) return <Loading />;
    if (error) return <AlertError error={error} />;
    return <RepoMetadata repo={repo} />;
};

const RepositoryMetadataPage = () => {
    const [setActiveTab] = useOutletContext();
    useEffect(() => setActiveTab('metadata'), [setActiveTab]);
    return <MetadataContainer />;
};

export default RepositoryMetadataPage;
