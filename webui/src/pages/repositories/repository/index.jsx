import React from "react";

import {Navigate, Route, Routes} from "react-router-dom";
import RepositoryObjectsPage from "./objects";
import RepositoryChangesPage from "./changes";
import RepositoryBranchesPage from "./branches";
import RepositoryTagsPage from "./tags";
import RepositoryComparePage from "./compare";
import RepositoryCommitsIndexPage from "./commits";
import RepositoryActionsIndexPage from "./actions";
import RepositoryGeneralSettingsPage from "./settings/general";
import RepositoryRetentionPage from "./settings/retention";
import RepositorySettingsBranchesPage from "./settings/branches";
import RepositoryObjectsViewPage from "./objectViewer";


const RepositoryPage = () => {
    return (
        <Routes>
            <Route path="objects/*" element={<RepositoryObjectsPage/>} />
            <Route path="object/*" element={<RepositoryObjectsViewPage />} />
            <Route path="changes/*" element={<RepositoryChangesPage/>} />
            <Route path="commits/*" element={<RepositoryCommitsIndexPage/>} />
            <Route path="branches/*" element={<RepositoryBranchesPage/>} />
            <Route path="tags/*" element={<RepositoryTagsPage/>} />
            <Route path="compare/*" element={<RepositoryComparePage/>} />
            <Route path="actions/*" element={<RepositoryActionsIndexPage/>} />
            <Route path="settings/*" element={<RepositoryGeneralSettingsPage/>} />
            <Route path="settings/general/*" element={<RepositoryGeneralSettingsPage/>} />
            <Route path="settings/retention/*" element={<RepositoryRetentionPage/>} />
            <Route path="settings/branches/*" element={<RepositorySettingsBranchesPage/>} />
            <Route path="/" element={<Navigate to="objects" />} />
        </Routes>
    )
};

export default RepositoryPage;
