package io.lakefs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractBondedFSContract;
import static io.lakefs.contract.TestLakeFSFileSystemContract.TEST_FS_LAKEFS_NAME;


public class LakeFSContract extends AbstractBondedFSContract {

    public static final String CONTRACT_XML = "contract/lakefs.xml";


    public LakeFSContract(Configuration conf) {
        super(conf);
        //insert the base features
        addConfResource(CONTRACT_XML);
    }

    @Override
    public String getScheme() {
        return "lakefs";
    }

    @Override
    public Path getTestPath() {
        return new Path(getConf().get(TEST_FS_LAKEFS_NAME) + "/main/test/");
    }
}
