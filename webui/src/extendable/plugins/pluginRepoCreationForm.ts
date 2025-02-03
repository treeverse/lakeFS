import React from "react";

type RepoCreationFormParams = {
    formID: string;
    config: object | Array<object>;
    formValid: unknown;
    setFormValid: unknown;
    onSubmit: unknown;
    error: null | undefined;
}

export interface PluginRepoCreationForm {
    build: (params: RepoCreationFormParams) => React.ReactElement;
    disableSampleRepoCreation: (params: object | Array<object>) => boolean;
}
