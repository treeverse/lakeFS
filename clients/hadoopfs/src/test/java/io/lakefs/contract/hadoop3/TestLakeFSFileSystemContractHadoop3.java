package io.lakefs.contract.hadoop3;

import io.lakefs.contract.TestLakeFSFileSystemContract;

public class TestLakeFSFileSystemContractHadoop3 extends TestLakeFSFileSystemContract {
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
}
