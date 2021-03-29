import {useRouter} from "next/router";
import {Alert} from "react-bootstrap";

import {useRepoAndRef} from "../../../lib/hooks/repo";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";
import RefDropdown from "../../../lib/components/repository/refDropdown";
import {ActionGroup, ActionsBar, Error, Loading} from "../../../lib/components/controls";



const RepositoryObjectBrowser = ({ repoId, onSelectRef, refId = null, path = null }) => {
    const { data, error } = useRepoAndRef(repoId, refId)
    if (!!error) return (<Error error={error}/> )
    if (!data) return (<Loading/>)

    const { repo, ref } = data;

    return (
        <RepositoryPageLayout repoId={repo.id} activePage={'objects'}>
            <ActionsBar>
                <ActionGroup orientation="left">
                    <RefDropdown
                        emptyText={'Select Branch'}
                        repo={repo}
                        selected={(!!ref) ? ref : null}
                        withCommits={true}
                        withWorkspace={true}
                        selectRef={onSelectRef}
                    />
                </ActionGroup>
            </ActionsBar>
        </RepositoryPageLayout>
    )
}

const RepositoryObjectsPage = () => {
    const router = useRouter()
    const { id, ref, path } = router.query;

    if (!id) return <Loading/>

    return (
        <RepositoryObjectBrowser
            repoId={id}
            refId={ref}
            path={path}
            onSelectRef={ref => router.push({
                pathname: `/repositories/[id]/objects`,
                query: {id, ref: ref.id}
            })}
        />
    )
}

export default RepositoryObjectsPage;