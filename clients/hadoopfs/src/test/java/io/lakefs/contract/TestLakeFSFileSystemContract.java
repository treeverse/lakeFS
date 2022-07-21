package io.lakefs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystemContractBaseTest;
import org.apache.hadoop.fs.Path;

import org.junit.Before;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();

    fs = LakeFSTestUtils.createTestFileSystem(conf);

    pathPrefix = conf.get(TEST_FS_LAKEFS_NAME) + "/main";
  }

  @Override
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.delete(prefixPath("/test"), true);
    }
    super.tearDown();
  }

  @Override
  public void testMkdirsWithUmask() throws Exception {
    // skip("Not supported");
  }

  protected Path prefixPath(String pathString) {
    return new Path(pathPrefix + pathString);
  }

  public void testRenameFileAsExistingFile() throws Exception {
    Path src = prefixPath("/test/hadoop/file");
    createFile(src);
    Path dst = prefixPath("/test/new/newfile");
    createFile(dst);

    rename(src, dst, true, false, true);
  }

  @Override
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    Path src = prefixPath("/test/hadoop/dir");
    fs.mkdirs(src);
    createFile(prefixPath("/test/hadoop/dir/file1"));
    createFile(prefixPath("/test/hadoop/dir/subdir/file2"));

    Path dst = prefixPath("/test/new/newdir");
    fs.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertFalse("Nested file1 exists",
        fs.exists(prefixPath("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists",
        fs.exists(prefixPath("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists",
        fs.exists(prefixPath("/test/new/newdir/file1")));
    assertTrue("Renamed nested exists",
        fs.exists(prefixPath("/test/new/newdir/subdir/file2")));
  }

  @Override
  public void testWorkingDirectory() throws Exception {
    // TODO make this test green and remove override
  }

}
