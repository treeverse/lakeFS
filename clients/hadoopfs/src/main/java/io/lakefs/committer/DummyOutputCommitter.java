package io.lakefs.committer;

import io.lakefs.LakeFSFileSystem;
import io.lakefs.LakeFSClient;
import io.lakefs.utils.ObjectLocation;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.BranchesApi;
import io.lakefs.clients.api.RefsApi;
import io.lakefs.clients.api.CommitsApi;
import io.lakefs.clients.api.model.BranchCreation;
import io.lakefs.clients.api.model.CommitCreation;

import org.apache.commons.lang.exception.ExceptionUtils; // (for debug prints ONLY)
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.http.HttpStatus;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

// TODO(ariels): For Hadoop 3, it is enough (and better!) to extend PathOutputCommitter.
public class DummyOutputCommitter extends FileOutputCommitter {
    private static final Logger LOG = LoggerFactory.getLogger(DummyOutputCommitter.class);


    /**
     * API client connected to the lakeFS server used on the filesystem.
     */
    protected LakeFSClient lakeFSClient = null;
    /**
     * Repository for files.
     */
    protected String repository = null;
    /**
     * Branch of output.  Defined on a task, null if this committer is a
     * noop.
     */
    protected String outputBranch = null;

    /**
     * Branch for this job.  Deleted if the job aborts.  Always defined,
     * even if this committer is a noop.
     *
     * TODO(ariels): Configurably delete even on job success.
     */
    protected String jobBranch = null;

    /**
     * Branch for this task.  Deleted if the task fails.  Always defined,
     * even if this committer is a noop.
     *
     * TODO(ariels): Configurably delete even on job success.
     */
    protected String taskBranch;

    /**
     * Output path (possibly supplied to task).  Defined on a task, null if
     * this committer is a noop.
     */
    protected Path outputPath = null;
    /**
     * "Working" path for this task: the same as outputPath, but on
     * taskBranch rather than on outputBranch.  Defined on a task, null if
     * this committer is a noop.
     */
    protected Path workPath = null;

    public DummyOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);
        // TODO(lynn): Create lakeFS API client.

        JobID id = context.getJobID();
        // BUG(ariels): Clean up job name.  It is selected by user and might
        // contain illegal characters.
        this.jobBranch = context.getJobName().isEmpty() ?
            id.toString() :
            String.format("%s-%s", context.getJobName(), id.toString());
        System.out.printf("TODO: Job branch: %s\n", jobBranch);

        if (outputPath != null) {
            Configuration conf = context.getConfiguration();
            FileSystem fs = outputPath.getFileSystem(conf);
            Preconditions.checkArgument(fs instanceof LakeFSFileSystem,
                                        "%s not on a LakeFSFileSystem", outputPath);
            this.lakeFSClient = new LakeFSClient(fs.getScheme(), conf);
            ObjectLocation outputLocation = ObjectLocation.pathToObjectLocation(null, outputPath);
            this.repository = outputLocation.getRepository();
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
        this.taskBranch = String.format("%s-%s", this.jobBranch, id.toString());

        if (outputPath != null) {
            ObjectLocation loc = ObjectLocation.pathToObjectLocation(null, outputPath);
            loc.setRef(this.taskBranch);
            this.workPath = loc.toFSPath();
            System.out.printf("TODO: Working path: %s\n", workPath);
        }
    }

    private boolean createBranch(String branch, String base) throws IOException {
        System.out.printf("TODO: Create branch %s from %s\n", branch, base);
        try {
            BranchesApi branches = lakeFSClient.getBranches();
            branches.createBranch(repository, new BranchCreation().name(branch).source(base));
            return true;
        } catch (ApiException e) {
            if (e.getCode() == HttpStatus.SC_CONFLICT) {
                LOG.debug("branch {} already exists", branch);
                return false;
                // TODO(ariels): Consider checking the branch originates
                //     from the expected location.  That will improve error
                //     handling if Spark ever supplies non-unique job (or
                //     task) IDs.
            }
            throw new IOException(String.format("createBranch %s from %s failed", branch, base), e);
        }
    }

    // TODO(ariels): Need to override getJobAttemptPath,
    // getPendingJobAttemptPath (which is *static*, what do we do???!?)

    @Override
    public Path getWorkPath() {
        System.out.printf("Get working path for %s -> %s\n%s\n", taskBranch, workPath,
                          ExceptionUtils.getStackTrace(new Throwable()));
        return workPath;
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        if (outputPath == null)
            return;
        LOG.debug("Setup job for %s on %s\n", outputBranch, jobBranch);
        createBranch(jobBranch, outputBranch);
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
        if (outputPath == null)
            return;
        LOG.debug("Commit job branch %s to %s\n", jobBranch, outputBranch);
        CommitsApi commits = lakeFSClient.getCommits();
        commits.commit(repository, jobBranch, new CommitCreation().message(String.format("commiting Job %s", jobContext.getJobID())));
        RefsApi refs = lakeFSClient.getRefs();
        refs.mergeIntoBranch(repository, jobBranch, outputBranch);
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State status) throws IOException { // TODO(lynn)
        if (outputPath == null)
            return;
        System.out.printf("TODO: Delete(?) job branch %s\n", jobBranch);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        return true;
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext)
        throws IOException {
        if (outputPath == null)
            return;
        LOG.debug("Setup task for %s on %s\n", taskBranch, jobBranch);
        createBranch(taskBranch, jobBranch);
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        if (outputPath == null)
            return;
        LOG.debug("Commit task branch %s to %s\n", taskBranch, jobBranch);
        CommitsApi commits = lakeFSClient.getCommits();
        commits.commit(repository, taskBranch, new CommitCreation().message(String.format("committing Task %s", taskContext.getTaskAttemptID())));
        RefsApi refs = lakeFSClient.getRefs();
        refs.mergeIntoBranch(repository, taskBranch, jobBranch);
    }

    public void abortTask(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        if (outputPath == null)
            return;
        System.out.printf("TODO: Delete task branch %s\n", taskBranch);
    }

    // TODO(lynn): More methods: isRecoverySupported, isCommitJobRepeatable, recoverTask.
}
