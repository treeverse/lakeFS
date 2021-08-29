package io.lakefs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileNotFoundException;

import static io.lakefs.contract.LakeFSTestUtils.intercept;
import static org.junit.Assume.assumeTrue;

/**
 *  Tests a live S3 system. If your keys and bucket aren't specified, all tests
 *  are marked as passed.
 *
 *  This uses BlockJUnit4ClassRunner because FileSystemContractBaseTest from
 *  TestCase which uses the old Junit3 runner that doesn't ignore assumptions
 *  properly making it impossible to skip the tests if we don't have a valid
 *  bucket.
 **/
public class TestLakeFSFileSystemContract extends FileSystemContractBaseTest {
  public static final String TEST_FS_LAKEFS_NAME = "test.fs.lakefs.name";

  private String pathPrefix;

  @Override
  public void setUp() throws Exception {
    Configuration conf = new Configuration();

    fs = LakeFSTestUtils.createTestFileSystem(conf);

    pathPrefix = conf.get(TEST_FS_LAKEFS_NAME) + "/main";
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(path("/test"), true);
    }
    super.tearDown();
  }

  @Override
  public void testMkdirsWithUmask() throws Exception {
    // skip("Not supported");
  }

  @Override
  protected Path path(String pathString) {
    return new Path(pathPrefix + pathString);
  }

  public void testRenameFileAsExistingFile() throws Exception {
    intercept(FileAlreadyExistsException.class, super::testRenameFileAsExistingFile);
  }

  @Override
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    if (!renameSupported()) {
      return;
    }

    Path src = path("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(path("/test/hadoop/dir/file1"));
    createFile(path("/test/hadoop/dir/subdir/file2"));

    Path dst = path("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertFalse("Nested file1 exists",
        fs.exists(path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
        fs.exists(path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        fs.exists(path("/test/new/newdir/file1")));
    assertTrue("Renamed nested exists",
        fs.exists(path("/test/new/newdir/subdir/file2")));
  }

  @Override
  public void testWorkingDirectory() throws Exception {
    // TODO make this test green and remove override
  }

  @Override
  public void testRenameDirectoryAsExistingFile() throws Exception {
    assumeTrue(renameSupported());

    Path src = path("/testRenameDirectoryAsExistingFile/dir");
    fs.mkdirs(src);
    Path dst = path("/testRenameDirectoryAsExistingFileNew/newfile");
    createFile(dst);
    intercept(FileAlreadyExistsException.class,
            () -> rename(src, dst, false, true, true));
  }

  @Override
  public void testRenameNonExistentPath() throws Exception {
    intercept(FileNotFoundException.class, super::testRenameNonExistentPath);
  }
}
