package io.lakefs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

public class TestLakeFSContractSeek extends AbstractContractSeekTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LakeFSContract(conf);
  }
}
