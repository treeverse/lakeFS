package io.lakefs.contract.hadoop3;

import io.lakefs.contract.TestLakeFSFileSystemContract;
import org.junit.Before;

public class TestLakeFSFileSystemContractHadoop3 extends TestLakeFSFileSystemContract {
    @Override
    @Before
    public void init() throws Exception {
        super.init();
    }

    public boolean rootDirTestEnabled() {
        return false;
    }

    public void testRenameDirToSelf() {
    }

    public void testRootDirAlwaysExists() {
    }

    public void testRenameFileAsExistingFile() {
    }

    public void testRenameDirectoryAsExistingDirectory() {
    }

    public void testRenameChildDirForbidden() {
    }

    public void testLSRootDir() throws Throwable {
    }

    public void testListStatusRootDir() throws Throwable {
    }
}
