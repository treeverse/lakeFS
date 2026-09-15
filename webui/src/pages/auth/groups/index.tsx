import React, { useEffect, useState } from 'react';
import Button from 'react-bootstrap/Button';

import { useAPIWithPagination } from '../../../lib/hooks/api';
import { auth } from '../../../lib/api';
import { ConfirmationButton } from '../../../lib/components/modals';
import { Paginator } from '../../../lib/components/pagination';
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    AlertError,
    FormattedDate,
    Loading,
    RefreshButton,
    useDebouncedState,
    SearchInput,
} from '../../../lib/components/controls';
import { useRouter } from '../../../lib/hooks/router';
import { Link } from '../../../lib/components/nav';
import { EntityActionModal } from '../../../lib/components/auth/forms';
import { disallowPercentSign, INVALID_GROUP_NAME_ERROR_MESSAGE } from '../validation';
import { useLoginConfigContext } from '../../../lib/hooks/conf';
import { useAuthOutletContext } from '../../../lib/components/auth/layout';
import { FeatureLockedEmptyState } from '../../../lib/components/auth/enterpriseUpgrade';

const GroupsContainer = () => {
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [refresh, setRefresh] = useState(false);

    const router = useRouter();
    const prefix = router.query.prefix ? router.query.prefix : '';
    const after = router.query.after ? router.query.after : '';

    const [searchPrefix, setSearchPrefix] = useDebouncedState(prefix, (search) =>
        router.push({ pathname: '/auth/groups', query: { prefix: search } }),
    );

    const { results, loading, error, nextPage } = useAPIWithPagination(() => {
        return auth.listGroups(prefix, after);
        // TODO: Review and remove this eslint-disable once dependencies are validated
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [refresh, prefix, after]);

    useEffect(() => {
        setSelected([]);
    }, [after, refresh]);

    if (error) return <AlertError error={error} />;
    if (loading) return <Loading />;
    const headers = ['', 'Group Name', 'Created At'];

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button variant="success" onClick={() => setShowCreate(true)}>
                        Create Group
                    </Button>

                    <ConfirmationButton
                        onConfirm={() => {
                            // eslint-disable-next-line @typescript-eslint/no-explicit-any
                            auth.deleteGroups(selected.map((g) => (g as any).id))
                                .catch((err) => setDeleteError(err))
                                .then(() => {
                                    setSelected([]);
                                    setRefresh(!refresh);
                                });
                        }}
                        disabled={selected.length === 0}
                        variant="danger"
                        msg={`Are you sure you'd like to delete ${selected.length} groups?`}
                    >
                        Delete Selected
                    </ConfirmationButton>
                </ActionGroup>
                <ActionGroup orientation="right">
                    <SearchInput
                        searchPrefix={searchPrefix}
                        setSearchPrefix={setSearchPrefix}
                        placeholder="Find a Group..."
                    />
                    <RefreshButton onClick={() => setRefresh(!refresh)} />
                </ActionGroup>
            </ActionsBar>
            <div className="auth-learn-more">
                A group is a collection of users.{' '}
                <a
                    href="https://docs.lakefs.io/reference/authorization/oss/#authorization"
                    target="_blank"
                    rel="noopener noreferrer"
                >
                    Learn more.
                </a>
            </div>

            {!!deleteError && <AlertError error={deleteError} />}

            <EntityActionModal
                show={showCreate}
                onHide={() => setShowCreate(false)}
                onAction={(groupName, groupDesc) => {
                    return auth.createGroup(groupName, groupDesc).then(() => {
                        setSelected([]);
                        setShowCreate(false);
                        setRefresh(!refresh);
                    });
                }}
                title="Create Group"
                placeholder="Group Name (e.g. 'DataTeam')"
                actionName={'Create'}
                validationFunction={disallowPercentSign(INVALID_GROUP_NAME_ERROR_MESSAGE)}
                showExtraField={true}
                extraPlaceholder="Group Description (optional)"
            />

            <DataTable
                results={results}
                headers={headers}
                keyFn={(group) => group.id}
                rowFn={(group) => {
                    const elements = [
                        <Checkbox
                            name={group.id}
                            onAdd={() => setSelected([...selected, group])}
                            onRemove={() => setSelected(selected.filter((g) => g !== group))}
                        />,
                        <Link
                            href={{
                                pathname: '/auth/groups/:groupId',
                                params: { groupId: group.id },
                            }}
                        >
                            {group.name}
                        </Link>,
                        <FormattedDate dateValue={group.creation_date} />,
                    ];

                    return elements;
                }}
                firstFixedCol={true}
            />

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={(after) => router.push({ pathname: '/auth/groups', query: { prefix, after } })}
            />
        </>
    );
};

export const GroupsPage = () => {
    const [setActiveTab] = useAuthOutletContext();
    const { RBAC: rbac } = useLoginConfigContext();
    useEffect(() => setActiveTab('groups'), [setActiveTab]);
    if (rbac === 'none') {
        return <FeatureLockedEmptyState feature="groups" />;
    }
    return <GroupsContainer />;
};

export default GroupsPage;
