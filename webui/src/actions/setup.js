import * as api from "./api";
import {AsyncActionType} from "./request";

export const SETUP_LAKEFS = new AsyncActionType('SETUP_LAKEFS');

export const doSetupLakeFS = (email, fullName) => SETUP_LAKEFS.execute(async () => api.setup.lakeFS(email, fullName));
