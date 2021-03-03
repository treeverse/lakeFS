import * as api from "./api";
import {AsyncActionType} from "./request";
import {DEFAULT_LISTING_AMOUNT} from "./api";

export const ACTIONS_RUNS = new AsyncActionType('ACTION_RUNS');
export const ACTIONS_RUN = new AsyncActionType('ACTIONS_RUN');
export const ACTIONS_RUN_HOOKS = new AsyncActionType('ACTION_RUN_HOOKS');
export const ACTIONS_RUN_HOOK_OUTPUT = new AsyncActionType('ACTIONS_RUN_HOOK_OUTPUT');

export const listActionsRuns = (repoId, after = "", amount = DEFAULT_LISTING_AMOUNT) =>
    ACTIONS_RUNS.execute(async () => api.actions.listRuns(repoId, after, amount));

export const getActionsRun = (repoId, runId) =>
    ACTIONS_RUN.execute(async () => api.actions.getRun(repoId, runId));

export const listActionsRunHooks = (repoId, runId) =>
    ACTIONS_RUN_HOOKS.execute(async () => api.actions.listRunHooks(repoId, runId, "", -1));


export const getActionsRunHookOutput = (repoId, runId, hookRunId) =>
    ACTIONS_RUN_HOOK_OUTPUT.execute(async () => api.actions.getRunHookOutput(repoId, runId, hookRunId));