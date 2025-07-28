import React from "react";
import { RendererComponent } from "../../pages/repositories/repository/fileRenderers/types";
import { ConfigType } from "../../lib/hooks/configProvider";

export interface PluginCustomObjectRenderers {
    init: (config: ConfigType | null) => void;
    get: (contentType: string | null, fileExtension: string | null) => React.FC<RendererComponent> | null;
}
