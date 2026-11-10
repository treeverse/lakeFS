export interface RendererComponent {
    repoId: string;
    refId: string;
    path: string;
    fileExtension?: string;
    contentType?: string;
    sizeBytes: number;
    presign?: boolean;
}

export interface RendererComponentWithText extends RendererComponent {
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
    TIFF,
    PDF,
    TEXT,
    UNSUPPORTED,
    TOO_LARGE,
    GEOJSON,
}
