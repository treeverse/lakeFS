import React from "react";

import {
  BrowserRouter as Router,
  Routes,
  Route,
  Navigate,
} from "react-router-dom";
import { WithLoginConfigContext } from "../lib/hooks/conf";

// pages
import RepositoriesPage from "./repositories";
import { RepositoryPageLayout } from "../lib/components/repository/layout.jsx";
import RepositoryObjectsPage from "./repositories/repository/objects";
import RepositoryObjectsViewPage from "./repositories/repository/objectViewer";
import RepositoryChangesPage from "./repositories/repository/changes";
import RepositoryCommitsIndexPage from "./repositories/repository/commits";
import RepositoryBranchesPage from "./repositories/repository/branches";
import RepositoryTagsPage from "./repositories/repository/tags";
import RepositoryComparePage from "./repositories/repository/compare";
import RepositoryActionsIndexPage from "./repositories/repository/actions";
import RepositoryGeneralSettingsPage from "./repositories/repository/settings/general";
import RepositoryRetentionPage from "./repositories/repository/settings/retention";
import RepositorySettingsBranchesPage from "./repositories/repository/settings/branches";
import { SettingsLayout } from "./repositories/repository/settings/layout";
import Layout from "../lib/components/layout";
import Auth from "./auth";
import Setup from "./setup";

export const IndexPage = () => {
  return (
    <Router>
      <WithLoginConfigContext>
        <Routes>
          <Route index element={<Navigate to="/repositories" />} />
          <Route path="repositories" element={<Layout />}>
            <Route index element={<RepositoriesPage />} />
            <Route path=":repoId/*" element={<RepositoryPageLayout />}>
              <Route path="objects/*" element={<RepositoryObjectsPage />} />
              <Route path="object/*" element={<RepositoryObjectsViewPage />} />
              <Route path="changes/*" element={<RepositoryChangesPage />} />
              <Route
                path="commits/*"
                element={<RepositoryCommitsIndexPage />}
              />
              <Route path="branches/*" element={<RepositoryBranchesPage />} />
              <Route path="tags/*" element={<RepositoryTagsPage />} />
              <Route path="compare/*" element={<RepositoryComparePage />} />
              <Route
                path="actions/*"
                element={<RepositoryActionsIndexPage />}
              />
              <Route path="settings" element={<SettingsLayout />}>
                <Route index element={<Navigate to="general" />} />
                <Route
                  path="general/*"
                  element={<RepositoryGeneralSettingsPage />}
                />
                <Route
                  path="retention/*"
                  element={<RepositoryRetentionPage />}
                />
                <Route
                  path="branches/*"
                  element={<RepositorySettingsBranchesPage />}
                />
              </Route>
              <Route index element={<Navigate to="objects" />} />
            </Route>
          </Route>
          <Route path="/auth/*" element={<Auth />} />
          <Route path="/setup/*" element={<Setup />} />
          <Route path="*" element={<Navigate to="/repositories" replace />} />
        </Routes>
      </WithLoginConfigContext>
    </Router>
  );
};
