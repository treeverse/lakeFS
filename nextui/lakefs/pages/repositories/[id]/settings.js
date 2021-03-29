import {useRouter} from "next/router";
import {RepositoryPageLayout} from "../../../lib/components/repository/layout";


const RepositorySettingsPage = () => {
    const router = useRouter()
    const { id } = router.query;

    return (
        <RepositoryPageLayout repoId={encodeURIComponent(id)} activePage={'settings'}>
            <h1>settings</h1>
        </RepositoryPageLayout>
    )
}

export default RepositorySettingsPage;