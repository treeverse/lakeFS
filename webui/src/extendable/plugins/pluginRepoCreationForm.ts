import React from "react";

type RepoCreationFormParams = {
    formID: string;
    configs: Array<object>;
    formValid: unknown;
    setFormValid: unknown;
    onSubmit: unknown;
    error: null | undefined;
}

export interface PluginRepoCreationForm {
    build: (params: RepoCreationFormParams) => React.ReactElement;
    allowSampleRepoCreationFunc: (configs: Array<object>) => boolean;
}
