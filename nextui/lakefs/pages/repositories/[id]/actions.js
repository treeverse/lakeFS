import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";


const RepositoryActionsPage = () => {
    const router = useRouter()
    const { id } = router.query;

    return (
        <RepositoryPageLayout repoId={encodeURIComponent(id)} activePage={'actions'}>
            <h1>actions</h1>
        </RepositoryPageLayout>
    )
}

export default RepositoryActionsPage;