import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";


const RepositoryActionsPage = () => {
    const router = useRouter()
    const { repoId } = router.query;

    return (
        <RepositoryPageLayout repoId={encodeURIComponent(repoId)} activePage={'actions'}>
            <h1>actions</h1>
        </RepositoryPageLayout>
    )
}

export default RepositoryActionsPage;