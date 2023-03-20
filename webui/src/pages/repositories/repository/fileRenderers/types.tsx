export interface RendererComponent {
    repoId: string;
    refId: string;
    path: string;
    fileExtension: string | null;
    contentType: string | null;
    sizeBytes: number;
    presign?: boolean;
}

export interface RendererComponentWithText extends RendererComponent{
    text: string;
}

export interface RendererComponentWithTextCallback extends RendererComponent {
    onReady: (text: string) => JSX.Element;
}

export enum FileType {
    DATA,
    MARKDOWN,
    IPYNB,
    IMAGE,
    PDF,
    TEXT,
    UNSUPPORTED,
    TOO_LARGE,
}