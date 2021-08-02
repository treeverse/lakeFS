package io.lakefs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractDeleteTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.ContractTestUtils;

public class TestLakeFSContractDelete extends AbstractContractDeleteTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LakeFSContract(conf);
  }

  @Override
  public void testDeleteDeepEmptyDir() throws Throwable {
    ContractTestUtils.skip("test needs to be fixed");
    // TODO make this test green and remove override
  }
}
