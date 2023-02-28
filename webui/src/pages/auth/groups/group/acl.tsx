import React, {useCallback, useEffect, useState} from "react";

import Button from "react-bootstrap/Button";
import Form from 'react-bootstrap/Form';

import {sortedUniq, without} from 'lodash';

import {GroupHeader} from "../../../../lib/components/auth/nav";
import {AuthLayout} from "../../../../lib/components/auth/layout";
import {auth, repositories} from "../../../../lib/api";
import {useAPI} from "../../../../lib/hooks/api";
import {AttachModal} from "../../../../lib/components/auth/forms";
import {ConfirmationButton} from "../../../../lib/components/modals";
import {
    ActionGroup,
    ActionsBar,
    DataTable,
    GrayOut,
    Loading,
    Error,
    RefreshButton
} from "../../../../lib/components/controls";
import {useRouter} from "../../../../lib/hooks/router";
import {Link} from "../../../../lib/components/nav";


const identity = (x) => x;

const GroupRepositoriesList = ({ groupId }) => {
    const [refresh, setRefresh] = useState(false);
    const [showAddModal, setShowAddModal] = useState(false);
    const [apiError, setAPIError] = useState(null);

    const {response: acl, loading, error: getError} = useAPI(
        async() => ({repositories: [], ...await auth.getACL(groupId)}),
        [groupId, refresh]);

    const hasACL = !!acl?.permission;
    // acl is results with the repositories field populated at least by an
    // empty list.  Refresh after updating it on the backend, to ensure the
    // display and results catch up.
    if (acl) {
        acl.repositories ||= [];
    }
    const allRepositories = acl?.all_repositories;
    console.log('all repos', allRepositories, acl);

    useEffect(() => {
        setAPIError(null);
    }, [refresh]);

    const updateACL = useCallback((newACL) => {
        if (newACL.repositories) {
            newACL.repositories = sortedUniq(newACL.repositories.sort());
        }
        return auth.putACL(groupId, newACL);
    }, [groupId]);

    const content = loading ? <Loading/> :
          getError ?  <Error error={getError}/> :
          (<>
               {apiError && <Error error={apiError}/>}

               {hasACL &&
                <GrayOut enabled={allRepositories}>
                    <DataTable
                        keyFn={identity}
                        rowFn={(repoId) => {
                            return [
                                <Link href={{pathname: '/repositories/:repoId', params: {repoId}}}>{repoId}</Link>, <b>{repoId}</b>
                            ];
                        }}
                        headers={['Repository']}
                        actions={[{
                            key: 'Remove',
                            buttonFn: (repoId, props) =><ConfirmationButton
                                                            size="sm"
                                                            variant="outline-danger"
                                                            msg={<span>Are you sure you{'\''}d like to remove permissions for repository <strong>{repoId}</strong> from group <strong>{groupId}</strong>?</span>}
                                                            onConfirm={() => {
                                                                acl.repositories = without(acl.repositories, repoId);
                                                                updateACL(acl).then(() => { setRefresh(!refresh); setAPIError(null); })
                                                                    .catch(e => setAPIError(e));
                                                            }}>
                                                            Remove
                                                        </ConfirmationButton>
                        }]}
                        results={acl?.repositories}
                        emptyState={allRepositories ? <></> : <>&empty;</>}
                    />
                </GrayOut>
               }

               {showAddModal &&
                <AttachModal
                    show={showAddModal}
                    emptyState={'No repositories'}
                    filterPlaceholder={'Find repository...'}
                    modalTitle={'Add to group ACL'}
                    addText={'Add to group'}
                    searchFn={prefix => repositories.list(prefix, "", 20)
                              .then(res => res.results.filter(r => !acl.repositories?.includes(r.id)))}
                    onHide={() => setShowAddModal(false)}
                    onAttach={(selected) => {
                        acl.repositories = acl.repositories.concat(selected);
                        updateACL(acl).then(() => { setRefresh(!refresh); setAPIError(null); })
                            .catch(e => setAPIError(e))
                            .finally(() => setShowAddModal(false));
                    }}/>
                }
            </>
        );

    return (
        <>
            <GroupHeader groupId={groupId} page={'acl'}/>

            {hasACL &&
             <Form.Check defaultChecked={allRepositories}
                         disabled={!hasACL}
                         type="checkbox"
                         label="All repositories"
                         onChange={(ev) => {
                             acl.all_repositories = ev.target.checked;
                             updateACL(acl).catch(e => setAPIError(e)).then(() => setRefresh(!refresh));
                         }}/>}

            <ActionsBar>
                <ActionGroup orientation="left">
                    <GrayOut enabled={allRepositories}>
                        <Button variant="success" onClick={() => setShowAddModal(true)}>Add Repositories</Button>
                    </GrayOut>
                </ActionGroup>

                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>

            <div className="mt-2">
                {content}
            </div>
        </>
    );
};

const GroupACLsContainer = () => {
    const router = useRouter();
    const { groupId } = router.params;
    return groupId && <GroupRepositoriesList groupId={groupId} />;
};

const GroupACLPage = () => {
    return (
        <AuthLayout activeTab="groups">
            <GroupACLsContainer/>
        </AuthLayout>
    );
};

export default GroupACLPage;
