package io.lakefs.committer;

import io.lakefs.LakeFSFileSystem;
import io.lakefs.LakeFSClient;
import io.lakefs.utils.ObjectLocation;

import org.apache.commons.lang.exception.ExceptionUtils; // (for debug prints ONLY)
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// TODO(ariels): For Hadoop 3, it is enough (and better!) to extend PathOutputCommitter.
public class DummyOutputCommitter extends FileOutputCommitter {
    private static final Logger LOG = LoggerFactory.getLogger(DummyOutputCommitter.class);

    protected String outputBranch = null;
    protected Path outputPath = null;
    protected String workBranch = null;
    protected Path workPath = null;

    public DummyOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);
        // TODO(lynn): Create lakeFS API client.

        if (outputPath != null) {
            Configuration conf = context.getConfiguration();
            FileSystem fs = outputPath.getFileSystem(conf);
            LakeFSClient lakeFSClient = new LakeFSClient(fs.getScheme(), conf)

            Preconditions.checkArgument(fs instanceof LakeFSFileSystem,
                                        "%s not on a LakeFSFileSystem", outputPath);
            ObjectLocation outputLocation = ObjectLocation.pathToObjectLocation(null, outputPath);
            this.outputBranch = outputLocation.getRef();
            this.outputPath = fs.makeQualified(outputPath);
        }
    }

    public DummyOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        this(outputPath, (JobContext)context);

        TaskAttemptID id = context.getTaskAttemptID();
        // TODO(ariels): s/[^-\w]//g on the branch ID, ensuring all chars
        // are allowed.  (Some) users might manage to create a bad task
        // name.
        this.workBranch = id.toString();
        if (outputPath != null) {
            createBranch(workBranch, outputBranch);

            ObjectLocation loc = ObjectLocation.pathToObjectLocation(null, outputPath);
            loc.setRef(this.workBranch);
            this.workPath = loc.toFSPath();
            System.out.printf("TODO: Working path: %s\n", workPath);
        }
    }

    private void createBranch(String branch, String base) { // TODO(lynn)
        System.out.printf("TODO: Create branch %s from %s\n", branch, base);
    }

    // TODO(ariels): Need to override getJobAttemptPath,
    // getPendingJobAttemptPath (which is *static*, what do we do???!?)

    @Override
    public Path getWorkPath() {
        System.out.printf("Get working path for %s -> %s\n%s\n", workBranch, workPath,
                          ExceptionUtils.getStackTrace(new Throwable()));
        return workPath;
    }

    @Override
    public void setupJob(JobContext context) throws IOException { // TODO(lynn)
        if (outputPath == null)
            return;
        System.out.printf("TODO: Setup job on %s\n", workBranch);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException { // TODO(lynn)
        if (outputPath == null)
            return;
        System.out.printf("TODO: Commit branch %s to %s\n", workBranch, outputBranch);
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State status) throws IOException { // TODO(lynn)
        if (outputPath == null)
            return;
        System.out.printf("TODO: Delete(?) branch %s\n", workBranch);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        return true;
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        if (outputPath == null)
            return;
        System.out.printf("TODO: Setup task on %s\n", workBranch);
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        if (outputPath == null)
            return;
        System.out.printf("TODO: Commit task %s\n", workBranch);
    }

    public void abortTask(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        if (outputPath == null)
            return;
        System.out.printf("TODO: Commit task %s\n", workBranch);
    }

    // TODO(lynn): More methods: isRecoverySupported, isCommitJobRepeatable, recoverTask.
}
