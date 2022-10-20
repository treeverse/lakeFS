package io.lakefs.contract;

import io.lakefs.LakeFSFileSystem;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.net.URI;

public class LakeFSTestUtils {

  public static final String NULL_RESULT = "(null)";

  public static LakeFSFileSystem createTestFileSystem(Configuration conf) throws IOException {
    String fsname = conf.getTrimmed(TestLakeFSFileSystemContract.TEST_FS_LAKEFS_NAME, "");

    boolean liveTest = !StringUtils.isEmpty(fsname);
    URI testURI = null;
    if (liveTest) {
      testURI = URI.create(fsname);
      liveTest = testURI.getScheme().equals("lakefs");
    }
    if (!liveTest) {
      // This doesn't work with our JUnit 3 style test cases, so instead we'll
      // make this whole class not run by default
      throw new AssumptionViolatedException(
          "No test filesystem in " + TestLakeFSFileSystemContract.TEST_FS_LAKEFS_NAME);
    }
    LakeFSFileSystem fs1 = new LakeFSFileSystem();
    //enable purging in tests
    conf.setBoolean(Constants.PURGE_EXISTING_MULTIPART, true);
    conf.setInt(Constants.PURGE_EXISTING_MULTIPART_AGE, 0);
    fs1.initialize(testURI, conf);
    return fs1;
  }

}
