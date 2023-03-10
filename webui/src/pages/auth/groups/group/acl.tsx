import React, {useCallback, useEffect, useState} from "react";

import Button from "react-bootstrap/Button";
import Form from 'react-bootstrap/Form';

import sortedUniq from 'lodash/sortedUniq';
import without from 'lodash/without';

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
    RefreshButton,
    WrapIf,
} from "../../../../lib/components/controls";
import {useRouter} from "../../../../lib/hooks/router";
import {Link} from "../../../../lib/components/nav";


const identity = (x) => x;

const GroupRepositoriesList = ({ groupId }) => {
    const [refresh, setRefresh] = useState(false);
    const [showAddModal, setShowAddModal] = useState(false);
    const [apiError, setAPIError] = useState(null);

    const {response: acl, loading, error: getError} = useAPI(
        async() => {
            return ({repositories: [], ...await auth.getACL(groupId)});
        },
        [groupId, refresh]);

    const hasACL = !!acl?.permission;
    // acl is results with the repositories field populated at least by an
    // empty list.  Refresh after updating it on the backend, to ensure the
    // display and results catch up.
    if (acl) {
        acl.repositories ||= [];
    }

    useEffect(() => {
        setAPIError(null);
    }, [refresh]);

    const removeRepoFromACL = useCallback((acl, groupId, repoId) => {
        acl.repositories = sortedUniq(without(acl.repositories, repoId).sort());
        return auth.putACL(groupId, acl)
            .then(() => { setRefresh(!refresh); setAPIError(null); })
            .catch(e => setAPIError(e));
    });

    const addReposToACL = useCallback((acl, groupId, repoIds) => {
        if (acl.repositories === undefined) {
            acl.repositories = [];
        }
        acl.repositories = sortedUniq(acl.repositories.concat(repoIds).sort());
        return auth.putACL(groupId, acl)
            .then(() => { setRefresh(!refresh); setAPIError(null); })
            .catch(e => setAPIError(e))
            .finally(() => setShowAddModal(false));
    });

    const setAllReposOnACL = useCallback((acl, groupId, all) => {
        acl.all_repositories = all;
        return auth.putACL(groupId, acl)
            .then(() => { setRefresh(!refresh); setAPIError(null); })
            .catch(e => setAPIError(e));
    });

    const content = loading ? <Loading/> :
          getError ?  <Error error={getError}/> :
          (<>
               {apiError && <Error error={apiError}/>}

               {hasACL &&
                <WrapIf Component={GrayOut} enabled={acl?.all_repositories}>
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
                            buttonFn: (repoId) =><ConfirmationButton
                                                     size="sm"
                                                     variant="outline-danger"
                                                     msg={<span>Are you sure you{'\''}d like to remove permissions for repository <strong>{repoId}</strong> from group <strong>{groupId}</strong>?</span>}
                                                     onConfirm={() => {
                                                         removeRepoFromACL(acl, groupId, repoId)
                                                     }}>
                                                     Remove
                                                 </ConfirmationButton>
                        }]}
                        results={acl?.repositories}
                        emptyState={acl?.all_repositories ? <></> : <>&empty;</>}
                    />
                </WrapIf>
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
                    onHide={setShowAddModal.bind(null, false)}
                    onAttach={addReposToACL.bind(null, acl, groupId)}/>
                }
            </>
        );

    return (
        <>
            <GroupHeader groupId={groupId} page={'acl'}/>

            {hasACL &&
             <Form.Check defaultChecked={acl?.all_repositories}
                         disabled={!hasACL}
                         type="checkbox"
                         label="All repositories"
                         onChange={(ev) => setAllReposOnACL(acl, groupId, ev.target.checked)}/>}

            <ActionsBar>
                <ActionGroup orientation="left">
                    <WrapIf Component={GrayOut} enabled={acl?.all_repositories}>
                        <Button variant="success" onClick={setShowAddModal.bind(null, true)}>Add Repositories</Button>
                    </WrapIf>
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
