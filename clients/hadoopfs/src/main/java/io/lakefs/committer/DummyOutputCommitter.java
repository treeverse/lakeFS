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
import io.lakefs.clients.api.model.DiffList;
import io.lakefs.clients.api.model.Merge;

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
        LOG.info("Construct OC: Job branch: {}", jobBranch);

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
            LOG.trace("Working path: {}", workPath);
        }
    }

    private boolean createBranch(String branch, String base) throws IOException {
        LOG.info("Create branch {} from {}", branch, base);
        try {
            BranchesApi branches = lakeFSClient.getBranches();
            branches.createBranch(repository, new BranchCreation().name(branch).source(base));
            return true;
        } catch (ApiException e) {
            if (e.getCode() == HttpStatus.SC_CONFLICT) {
                LOG.info("branch {} already exists", branch);
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
        return workPath;
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        if (outputPath == null)
            return;
        LOG.info("Setup job for {} on {}", outputBranch, jobBranch);
        createBranch(jobBranch, outputBranch);
    }

    protected boolean hasChanges(String branch) throws IOException {
        try {
            BranchesApi branches = lakeFSClient.getBranches();
            DiffList res = branches.diffBranch(repository, branch, null, 1, null, null);
            return res.getResults().size() > 0;
        } catch (ApiException e) {
            throw new IOException(String.format("hasChanges(%s, %s): diffBranch failed", repository, branch), e);
        }
    }

    protected boolean hasDiffs(String repository, String from, String to) throws IOException {
        try {
            RefsApi refs = lakeFSClient.getRefs();
            DiffList res = refs.diffRefs(repository, from, to, null, 1, null, null, "two_dot", "two_dot");
            return res.getResults().size() > 0;
        } catch (ApiException e) {
            throw new IOException(String.format("hasDiffs(%s, %s => %s): diff failed", repository, from, to), e);
        }
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
        if (outputPath == null)
            return;
        try {
            if (!hasDiffs(repository, jobBranch, outputBranch)) {
                LOG.debug("No differences from {} to {}, nothing to merge", jobBranch, outputBranch);
                return;
            }
            LOG.info("Commit job branch {} to {}", jobBranch, outputBranch);
            RefsApi refs = lakeFSClient.getRefs();
            refs.mergeIntoBranch(repository, jobBranch, outputBranch, new Merge().message("").strategy("source-wins"));
        } catch (ApiException e) {
            throw new IOException(String.format("commitJob %s failed", jobContext.getJobID()), e);
        }
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State status) throws IOException { // TODO(lynn)
        if (outputPath == null)
            return;
        LOG.info("TODO: Delete(?) job branch {}", jobBranch);
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
        LOG.info("Setup task for {} on {}", taskBranch, jobBranch);
        createBranch(jobBranch, outputBranch);
        createBranch(taskBranch, jobBranch);
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext)
        throws IOException {
        if (outputPath == null)
            return;
        try {
            if (!hasChanges(taskBranch)) {
                LOG.debug("Nothing to commit into {} for {}", taskBranch, jobBranch);
                return;
            }

            LOG.info("Commit task branch {} and merge to {}", taskBranch, jobBranch);
            CommitsApi commits = lakeFSClient.getCommits();
            commits.commit(repository, taskBranch, new CommitCreation().message(String.format("committing Task %s", taskContext.getTaskAttemptID())), null);

            if (!hasDiffs(repository, taskBranch, jobBranch)) {
                LOG.info("Strange, after committing no differences from {} to {}, nothing to merge", taskBranch, jobBranch);
                return;
            }
            RefsApi refs = lakeFSClient.getRefs();
            refs.mergeIntoBranch(repository, taskBranch, jobBranch, new Merge().message(""));
        } catch (ApiException e) {
            throw new IOException(String.format("commitTask %s failed", taskContext.getTaskAttemptID()), e);
        }
    }

    public void abortTask(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        if (outputPath == null)
            return;
        LOG.info("TODO: Delete task branch %s", taskBranch);
    }

    // TODO(lynn): More methods: isRecoverySupported, isCommitJobRepeatable, recoverTask.
}
