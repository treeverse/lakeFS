package io.lakefs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.net.URI;

/**
 * A dummy implementation of the core Lakefs Filesystem.
 * This class implements a {@link LakeFSFileSystem} that can be registered to Spark and support limited write and read actions.
 */
public class LakeFSFileSystem extends org.apache.hadoop.fs.FileSystem {

    @Override
    public URI getUri() {
        return URI.create("lakefs://main");
    }

    /**
     *{@inheritDoc}
     * Called on a file read Spark action. This method returns a FSDataInputStream with a static string,
     * regardless of the given file path.
     */
    @Override
    public FSDataInputStream open(Path path, int i) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ Calling open method for: " + path.getName() + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        String strToWrite = "abc";
        MyInputStream inputStream = new MyInputStream(strToWrite);
        return new FSDataInputStream(inputStream);
    }

    /**
     *{@inheritDoc}
     * Called on a file write Spark/Hadoop action. This method writes the content of the file in path into stdout.
     */
    @Override
    public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i1, long l,
                                     Progressable progressable) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ create path: " + path.toString() + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        return new FSDataOutputStream(System.out, null);
    }

    @Override
    public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
        throw new UnsupportedOperationException("Append is not supported by LakeFSFileSystem");
    }

    @Override
    public boolean rename(Path path, Path path1) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ rename $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        return false;
    }

    @Override
    public boolean delete(Path path, boolean b) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ delete $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        return false;
    }

    @Override
    public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ List status is called for: " + path.toString() + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(Path path) {
        LOG.info("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ setWorkingDirectory $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
    }

    @Override
    public Path getWorkingDirectory() {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ getWorkingDirectory $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        return new Path("/workdir");
    }

    @Override
    public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ mkdirs $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        return false;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ getFileStatus, path: " + path.toString() + " $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        FileStatus fStatus = new FileStatus(0, false, 1, 20, 1, path);
        return fStatus;
    }

    @Override
    public void initialize(URI name, Configuration conf) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ initialize $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        super.initialize(name, conf);
    }

    /**
     * Return the protocol scheme for the FileSystem.
     *
     * @return "lakefs"
     */
    @Override
    public String getScheme() {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ getScheme $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        return "lakefs";
    }

    @Override
    public FileStatus[] globStatus(Path pathPattern) throws IOException {
        LOG.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$ globStatus $$$$$$$$$$$$$$$$$$$$$$$$$$$$ ");
        FileStatus fStatus = new FileStatus(0, false, 1, 20, 1,
                new Path("tal-test"));
        FileStatus[] res = new FileStatus[1];
        res[0] = fStatus;
        return res;
    }

    @Override
    public boolean exists(Path f) throws IOException {
        return false;
    }


    /**
     * An {@link InputStream} designated to serve as an input to the {@link FSDataInputStream} constructor. To be a
     * viable input for FSDataInputStream, this class must be an instance of {@link InputStream} (StringBufferInputStream
     * inherits it), and it must implement the interfaces {@link Seekable} and {@link PositionedReadable}.
     *
     * The read logic is implemented in {@link StringBufferInputStream#read()}.
     */
    private class MyInputStream extends StringBufferInputStream implements Seekable,PositionedReadable {

        public MyInputStream(String input) {
            super(input);
            LOG.debug("--------------------------- ctor ---------------------------");
        }

        @Override
        public int read(long l, byte[] bytes, int i, int i1) throws IOException {
            LOG.debug("--------------------------- read1 ---------------------------");
            return 1;
        }

        @Override
        public void readFully(long l, byte[] bytes, int i, int i1) throws IOException {
            LOG.debug("--------------------------- readFully1---------------------------");
        }

        @Override
        public void readFully(long l, byte[] bytes) throws IOException {
            LOG.debug("--------------------------- readFully2 ---------------------------");
        }

        @Override
        public void seek(long l) throws IOException {
            LOG.debug("--------------------------- seek ---------------------------");
        }

        @Override
        public long getPos() throws IOException {
            LOG.debug("--------------------------- getPos---------------------------");
            return 0;
        }

        @Override
        public boolean seekToNewSource(long l) throws IOException {
            LOG.debug("--------------------------- seekToNewSource---------------------------");
            return false;
        }
    }
}
