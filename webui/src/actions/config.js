import * as api from "./api";
import {AsyncActionType} from "./request";

export const CONFIG_GET = new AsyncActionType('CONFIG_GET');

export const getConfig = () => CONFIG_GET.execute(api.config.get);