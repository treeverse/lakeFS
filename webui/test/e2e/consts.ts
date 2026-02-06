import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const COMMON_STORAGE_STATE_PATH = path.join(__dirname, "../playwright/.auth/common-storage-state.json");
export const RAW_CREDENTIALS_FILE_PATH = path.join(__dirname, "../playwright/.auth/credentials.json");
