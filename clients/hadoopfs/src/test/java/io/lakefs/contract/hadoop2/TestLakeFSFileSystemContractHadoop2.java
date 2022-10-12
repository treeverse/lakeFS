package io.lakefs.contract.hadoop2;

import io.lakefs.contract.TestLakeFSFileSystemContract;
import org.apache.hadoop.fs.Path;

public class TestLakeFSFileSystemContractHadoop2 extends TestLakeFSFileSystemContract {
    @Override
    protected Path path(String pathString) {
        return new Path(pathPrefix + pathString);
    }
}
