package io.lakefs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;

import static org.apache.hadoop.fs.contract.ContractTestUtils.dataset;
import static org.apache.hadoop.fs.contract.ContractTestUtils.writeDataset;

public class TestLakeFSContractRename extends AbstractContractRenameTest {

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new LakeFSContract(conf);
  }

  @Override
  public void testRenameDirIntoExistingDir() throws Throwable {
    describe("Verify renaming a dir into an existing dir puts the files"
             +" from the source dir into the existing dir"
             +" and leaves existing files alone");
    FileSystem fs = getFileSystem();
    String sourceSubdir = "source";
    Path srcDir = path(sourceSubdir);
    Path srcFilePath = new Path(srcDir, "source-256.txt");
    byte[] srcDataset = dataset(256, 'a', 'z');
    writeDataset(fs, srcFilePath, srcDataset, srcDataset.length, 1024, false);
    Path destDir = path("dest");

    Path destFilePath = new Path(destDir, "dest-512.txt");
    byte[] destDateset = dataset(512, 'A', 'Z');
    writeDataset(fs, destFilePath, destDateset, destDateset.length, 1024,
        false);
    assertIsFile(destFilePath);

    boolean rename = fs.rename(srcDir, destDir);
    assertFalse("s3a doesn't support rename to non-empty directory", rename);
  }
}
