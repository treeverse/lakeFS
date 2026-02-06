import { APIRequestContext } from "@playwright/test";
import { LakeFSCredentials } from "./types";

export class LakeFSApi {
    constructor(
        private request: APIRequestContext,
        private baseUrl: string,
        private credentials?: LakeFSCredentials,
    ) {}

    private extraHeaders(): Record<string, string> {
        if (!this.credentials) {
            return {};
        }
        const token = Buffer.from(`${this.credentials.accessKeyId}:${this.credentials.secretAccessKey}`).toString("base64");
        return { Authorization: `Basic ${token}` };
    }

    private url(path: string): string {
        return `${this.baseUrl}/api/v1${path}`;
    }

    private async assertOk(resp: Awaited<ReturnType<APIRequestContext["get"]>>, action: string) {
        if (!resp.ok()) {
            const body = await resp.text();
            throw new Error(`${action} failed: ${resp.status()} ${resp.statusText()} - ${body}`);
        }
    }

    async createRepository(name: string, storageNamespace: string, opts?: { defaultBranch?: string; readOnly?: boolean; ifNotExists?: boolean }) {
        const resp = await this.request.post(this.url("/repositories"), {
            headers: this.extraHeaders(),
            data: {
                name,
                storage_namespace: storageNamespace,
                default_branch: opts?.defaultBranch ?? "main",
                read_only: opts?.readOnly ?? false,
            },
        });
        if (opts?.ifNotExists && resp.status() === 409) {
            return;
        }
        await this.assertOk(resp, `createRepository(${name})`);
        return resp.json();
    }

    async deleteRepository(name: string) {
        const resp = await this.request.delete(this.url(`/repositories/${name}`), {
            headers: this.extraHeaders(),
        });
        await this.assertOk(resp, `deleteRepository(${name})`);
    }

    async createBranch(repository: string, name: string, source = "main") {
        const resp = await this.request.post(this.url(`/repositories/${repository}/branches`), {
            headers: this.extraHeaders(),
            data: { name, source },
        });
        await this.assertOk(resp, `createBranch(${repository}, ${name})`);
        return resp.text();
    }

    async uploadObject(repository: string, branch: string, path: string, content: string | Buffer) {
        const resp = await this.request.post(
            this.url(`/repositories/${repository}/branches/${branch}/objects?path=${encodeURIComponent(path)}`),
            {
                headers: {
                    ...this.extraHeaders(),
                    "Content-Type": "application/octet-stream",
                },
                data: content,
            }
        );
        await this.assertOk(resp, `uploadObject(${repository}, ${branch}, ${path})`);
        return resp.json();
    }

    async commit(repository: string, branch: string, message: string, metadata?: Record<string, string>) {
        const resp = await this.request.post(this.url(`/repositories/${repository}/branches/${branch}/commits`), {
            headers: this.extraHeaders(),
            data: {
                message,
                metadata: metadata ?? {},
            },
        });
        await this.assertOk(resp, `commit(${repository}, ${branch})`);
        return resp.json();
    }

    async merge(repository: string, sourceRef: string, destinationBranch: string, message?: string) {
        const resp = await this.request.post(
            this.url(`/repositories/${repository}/refs/${sourceRef}/merge/${destinationBranch}`),
            {
                headers: this.extraHeaders(),
                data: { message: message ?? "" },
            }
        );
        await this.assertOk(resp, `merge(${repository}, ${sourceRef} -> ${destinationBranch})`);
        return resp.json();
    }

    async deleteObject(repository: string, branch: string, path: string) {
        const resp = await this.request.delete(
            this.url(`/repositories/${repository}/branches/${branch}/objects?path=${encodeURIComponent(path)}`),
            {
                headers: this.extraHeaders(),
            }
        );
        await this.assertOk(resp, `deleteObject(${repository}, ${branch}, ${path})`);
    }
}
