import React from "react";

import { Navigate, Route, Routes } from "react-router-dom";
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
import { RefContextProvider } from "../../../lib/hooks/repo";
import { StorageConfigProvider } from "../../../lib/hooks/storageConfig";

const RepositoryPage = () => {
  return (
    <StorageConfigProvider>
      <Routes></Routes>
    </StorageConfigProvider>
  );
};

export default RepositoryPage;
