import React from "react";
import { RendererComponent } from "../../pages/repositories/repository/fileRenderers/types";
import { ConfigType } from "../../lib/hooks/configProvider";

export interface PluginCustomObjectRenderers {
    init: (config?: ConfigType) => void;
    get: (contentType?: string, fileExtension?: string) => React.FC<RendererComponent> | null;
}
