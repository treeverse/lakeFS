import {useEffect, useRef, useState} from "react";
import {useRouter} from "next/router";
import Link from 'next/link';
import {auth} from "../../../rest/api";
import Button from "react-bootstrap/Button";
import Modal from "react-bootstrap/Modal";
import Form from "react-bootstrap/Form";
import {FormControl} from "react-bootstrap";

import {useAPIWithPagination} from "../../../rest/hooks";
import {AuthLayout} from "../../../lib/components/auth/layout";
import {
    ActionGroup,
    ActionsBar,
    Checkbox,
    DataTable,
    Error, FormattedDate,
    Loading,
    RefreshButton
} from "../../../lib/components/controls";
import {ConfirmationButton} from "../../../lib/components/modals";
import {Paginator} from "../../../lib/components/pagination";
import {PolicyEditor} from "../../../lib/components/auth/policy";

const PoliciesContainer = () => {
    const [selected, setSelected] = useState([]);
    const [deleteError, setDeleteError] = useState(null);
    const [showCreate, setShowCreate] = useState(false);
    const [refresh, setRefresh] = useState(false);

    const router = useRouter();
    const after = (!!router.query.after) ? router.query.after : "";
    const { results, loading, error, nextPage } =  useAPIWithPagination(() => {
        return auth.listPolicies("", after);
    }, [after, refresh]);

    if (!!error) return <Error error={error}/>;
    if (loading) return <Loading/>;

    return (
        <>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <Button
                        variant="success"
                        onClick={() => setShowCreate(true)}>
                        Create Policy
                    </Button>

                    <ConfirmationButton
                        onConfirm={() => {
                            auth.deletePolicies(selected.map(p => p.id))
                                .catch(err => setDeleteError(err))
                                .then(() => {
                                    setSelected([]);
                                    setRefresh(!refresh);
                                });
                        }}
                        disabled={(selected.length === 0)}
                        variant="danger"
                        msg={`Are you sure you'd like to delete ${selected.length} policies?`}>
                        Delete Selected
                    </ConfirmationButton>

                </ActionGroup>
                <ActionGroup orientation="right">
                    <RefreshButton onClick={() => setRefresh(!refresh)}/>
                </ActionGroup>
            </ActionsBar>

            {(!!deleteError) && <Error error={deleteError}/>}

            <PolicyEditor
                onSubmit={(policyId, policyBody) => {
                    return auth.createPolicy(policyId, policyBody).then(() => {
                        setShowCreate(false);
                        setRefresh(!refresh);
                    })
                }}
                onHide={() => setShowCreate(false)}
                show={showCreate}
            />

            <DataTable
                results={results}
                headers={['', 'Policy ID', 'Created At']}
                keyFn={policy => policy.id}
                rowFn={policy => [
                    <Checkbox
                        name={policy.id}
                        onAdd={() => setSelected([...selected, policy])}
                        onRemove={() => setSelected(selected.filter(p => p !== policy))}
                    />,
                    <Link href={{pathname: '/auth/policies/[policyId]', query: {policyId: policy.id}}}>
                        {policy.id}
                    </Link>,
                    <FormattedDate dateValue={policy.creation_date}/>
                ]}/>

            <Paginator
                nextPage={nextPage}
                after={after}
                onPaginate={after => router.push({pathname: '/auth/policies', query: {after}})}
            />
        </>
    );
};


const PoliciesPage = () => {
    return (
        <AuthLayout activeTab="policies">
            <PoliciesContainer/>
        </AuthLayout>
    );
};

export default PoliciesPage;