import React from "react";

import { PluginRepoCreationForm } from "../pluginRepoCreationForm";
import { RepositoryCreateForm } from "../../../lib/components/repositoryCreateForm";

const DefaultRepoCreationFormPlugin: PluginRepoCreationForm = {
    build: ({formID, config, formValid, setFormValid, onSubmit, error}) => {
        return (
            <RepositoryCreateForm
                formID={formID}
                config={config}
                formValid={formValid}
                setFormValid={setFormValid}
                onSubmit={onSubmit}
                error={error}
            />
        );
    },
    allowSampleRepoCreationFunc: () => {
        return true;
    },
}

export default DefaultRepoCreationFormPlugin;

