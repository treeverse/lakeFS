import fs from "fs/promises";
import {RAW_CREDENTIALS_FILE_PATH} from "./consts";

interface Credentials {
    accessKeyId: string;
    secretAccessKey: string;
}

export const getCredentials = async (): Promise<Credentials|null> => {
    try {
        return JSON.parse(await fs.readFile(RAW_CREDENTIALS_FILE_PATH, "utf-8"));
    } catch (e) {
        if (e.code === "ENOENT") {
            return null;
        }
        throw e;
    }
}

export const writeCredentials = async (credentials: Credentials): Promise<void> => {
    const jsonCredentials = JSON.stringify(credentials);
    await fs.writeFile(RAW_CREDENTIALS_FILE_PATH, jsonCredentials);
}