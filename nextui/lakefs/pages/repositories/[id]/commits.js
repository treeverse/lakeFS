import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";


const RepositoryCommitsPage = () => {
    const router = useRouter()
    const { id } = router.query;

    return (
        <RepositoryPageLayout repoId={encodeURIComponent(id)} activePage={'commits'}>
            <h1>commits</h1>
        </RepositoryPageLayout>
    )
}

export default RepositoryCommitsPage;