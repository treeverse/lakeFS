import React from "react";

import { PluginRepoCreationForm } from "../pluginRepoCreationForm";
import { RepositoryCreateForm } from "../../../lib/components/repositoryCreateForm";

const DefaultRepoCreationFormPlugin: PluginRepoCreationForm = {
    build: ({formID, configs, formValid, setFormValid, onSubmit, error}) => {
        const config = configs && configs.length ? configs[0] : null;
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

