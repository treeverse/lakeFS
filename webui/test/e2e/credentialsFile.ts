import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";
import { LakeFSCredentials } from "./types";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CREDENTIALS_PATH = path.join(__dirname, "../playwright/credentials.json");
export const STORAGE_STATE_PATH = path.join(__dirname, "../playwright/common-storage-state.json");

export const getCredentials = async (): Promise<LakeFSCredentials|null> => {
    try {
        return JSON.parse(await fs.readFile(CREDENTIALS_PATH, "utf-8"));
    } catch (e: unknown) {
        if (e instanceof Error && (e as NodeJS.ErrnoException).code === "ENOENT") {
            return null;
        }
        throw e;
    }
}

export const ensureStorageDir = async (): Promise<void> => {
    await fs.mkdir(path.dirname(STORAGE_STATE_PATH), { recursive: true });
}

export const writeCredentials = async (credentials: LakeFSCredentials): Promise<void> => {
    await fs.mkdir(path.dirname(CREDENTIALS_PATH), { recursive: true });
    const jsonCredentials = JSON.stringify(credentials);
    await fs.writeFile(CREDENTIALS_PATH, jsonCredentials);
}
