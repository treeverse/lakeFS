package io.lakefs.storage;

import org.apache.hadoop.fs.Path;

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

    // translate a physical address in lakeFS storage namespace syntax into a Hadoop FileSystem Path
    public Path translate(String address) throws URISyntaxException {
        if(!Pattern.compile(validityRegex).matcher(address).find()) {
            throw new RuntimeException(String.format("address %s does not match blockstore namespace regex %s", address,
            validityRegex));
        }
        
        // Going through Path.toUri to avoid encoding bugs. See: https://github.com/treeverse/lakeFS/issues/5827
        URI uri = new Path(address).toUri();
        switch (blockstoreType) {
            case "s3":
                return new Path(new URI("s3a", uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()));
            case "azure":
                // TODO(johnnyaug) - translate https:// style to abfs://
            default:
                throw new RuntimeException(String.format("lakeFS blockstore type %s unsupported by this FileSystem", blockstoreType));
        }
    }
}
