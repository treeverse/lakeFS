package io.lakefs.contract;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractMkdirTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

/**
 * Test dir operations on S3
 */
public class TestLakeFSContractMkdir extends AbstractContractMkdirTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LakeFSContract(conf);
  }
}
