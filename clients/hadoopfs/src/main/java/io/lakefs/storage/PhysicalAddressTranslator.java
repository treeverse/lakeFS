package io.lakefs.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

public class PhysicalAddressTranslator {
    private String blockstoreType;
    private String validityRegex;

    public PhysicalAddressTranslator(String blockstoreType, String validityRegex) {
        this.blockstoreType = blockstoreType;
        this.validityRegex = validityRegex;
    }

    // translate a URI in lakeFS storage namespace syntax into a Hadoop FileSystem URI
    public URI translate(URI uri) throws URISyntaxException {
        if(!Pattern.compile(validityRegex).matcher(uri.toString()).find()) {
            throw new RuntimeException(String.format("URI %s does not match blockstore namespace regex %s", uri.toString(),
            validityRegex));
        }
        switch (blockstoreType) {
            case "s3":
                return new URI("s3a", uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(),
                        uri.getFragment());
            case "azure":
                // TODO(johnnyaug) - translate https:// style to abfs://
            default:
                throw new RuntimeException(String.format("lakeFS blockstore type %s unsupported by this FileSystem", blockstoreType));
        }
    }
}
