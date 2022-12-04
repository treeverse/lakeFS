package io.lakefs.committer;

import io.lakefs.Constants;
import io.lakefs.FSConfiguration;
import io.lakefs.LakeFSFileSystem;
import io.lakefs.LakeFSClient;
import io.lakefs.utils.Bulk;
import io.lakefs.utils.ObjectLocation;

import io.lakefs.clients.api.ApiException;
import io.lakefs.clients.api.BranchesApi;
import io.lakefs.clients.api.RefsApi;
import io.lakefs.clients.api.CommitsApi;
import io.lakefs.clients.api.model.BranchCreation;
import io.lakefs.clients.api.model.Commit;
import io.lakefs.clients.api.model.CommitCreation;
import io.lakefs.clients.api.model.DiffList;
import io.lakefs.clients.api.model.Merge;

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
import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;

// TODO(ariels): For Hadoop 3, it is enough (and better!) to extend PathOutputCommitter.
public class LakeFSOutputCommitter extends FileOutputCommitter {
    private static final Logger LOG = LoggerFactory.getLogger(LakeFSOutputCommitter.class);

    private static final String branchNamePrefix = "lakeFS-OC";

    /**
     * API client connected to the lakeFS server used on the filesystem.
     */
    protected LakeFSClient lakeFSClient = null;

    protected Bulk bulk = null;

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
     */
    protected String jobBranch = null;

    /**
     * ID for this task, if created with a task context.  Always defined,
     * even if this committer is a noop.
     */
    protected String taskID = null;

    /**
     * Output path (possibly supplied to task).  Defined on a task, null if
     * this committer is a noop.
     */
    protected Path outputPath = null;

    /**
     * Path for this job: outputPath in the job branch.
     */
    protected Path jobPath = null;

    /**
     * "Working" path for this task: the same as outputPath, but on
     * jobBranch rather than on outputBranch, and under
     * _temporary/<TASK_ID>.  Defined on a task, null if this committer is a
     * noop.
     */
    protected Path workPath = null;

    protected Configuration conf;
    protected FileSystem fs;

    private static HashFunction hash = Hashing.sha256();
    private static Charset utf8 = Charset.forName("utf8");

    /*
     * BUG(ariels): This is based on the outputPath, the only thing that
     *     works.  It will need to be changed for multiwriter -- need to
     *     depend on a configured algorithm here!
     */
    protected String pathToBranch(Path p) {
        String path = p.toString();
        // TODO(ariels): Use a more compact encoding (base-36?)
        String digest = new BigInteger(hash.hashString(path, utf8).asBytes()).toString(36);
        String pathPrefix = path.length() > 128 ? path.substring(0, 128) : path;
        pathPrefix = pathPrefix.replaceAll("[^-_a-zA-Z0-9]", "-");
        return String.format("%s-%s-%s", branchNamePrefix, digest, pathPrefix);
    }

    public LakeFSOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);

        this.conf = context.getConfiguration();

        this.jobBranch = pathToBranch(outputPath);

        LOG.info("Construct OC: Job branch: {}", jobBranch);

        if (outputPath != null) {
            fs = outputPath.getFileSystem(conf);
            Preconditions.checkArgument(fs instanceof LakeFSFileSystem,
                                        "%s not on a LakeFSFileSystem", outputPath);
            this.lakeFSClient = new LakeFSClient(fs.getScheme(), conf);
            this.bulk = new Bulk(this.lakeFSClient);
            ObjectLocation outputLocation = ObjectLocation.pathToObjectLocation(null, outputPath);
            this.repository = outputLocation.getRepository();
            this.outputBranch = outputLocation.getRef();
            this.outputPath = fs.makeQualified(outputPath);
            this.jobPath = changePathBranch(this.outputPath, this.jobBranch);
        }
    }

    public LakeFSOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        this(outputPath, (JobContext)context);

        TaskAttemptID id = context.getTaskAttemptID();
        // TODO(ariels): Consider removing weird characters from the task
        //     ID: users who do not use Spark might manage to create a weird
        //     task name.
        this.taskID = id.toString();

        if (outputPath != null) {
            this.workPath = new Path(this.jobPath, "_temporary/" + this.taskID + "/");
            LOG.debug("Working path: {}", this.workPath);
        }
    }

    private Path changePathBranch(Path path, String branch) {
        ObjectLocation loc = ObjectLocation.pathToObjectLocation(path);
        loc.setRef(branch);
        return loc.toFSPath();
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

    private void deleteBranch(String branch) throws IOException {
        LOG.info("Cleanup branch {}", branch);
        try {
            BranchesApi branches = lakeFSClient.getBranches();
            branches.deleteBranch(repository, branch);
        } catch (ApiException e) {
            throw new IOException(String.format("deleteBranch %s failed", branch), e);
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
        if (FSConfiguration.getBoolean(conf, outputPath.toUri().getScheme(), Constants.OC_ENSURE_CLEAN_OUTPUT_BRANCH, true) &&
            hasChanges(outputBranch)) {
            throw new IOException(String.format("Uncommitted changes on output branch %s, merge will fail", outputBranch));
        }
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

    private void cleanupJob() throws IOException {
        if (FSConfiguration.getBoolean(conf, outputPath.toUri().getScheme(), Constants.OC_DELETE_JOB_BRANCH, true)) {
            deleteBranch(jobBranch);
        }
    }

    private boolean needsSuccessFile() {
        String markSuccessfulJobs = FSConfiguration.get(conf, outputPath.toUri().getScheme(), Constants.OC_SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, "auto");
        if (markSuccessfulJobs.equals("auto")) {
            return conf.getBoolean(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true);
        }
        if (markSuccessfulJobs.equalsIgnoreCase("false")) {
            return false;
        }
        return true;
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
        if (outputPath == null)
            return;
        try {
            boolean changes = false;

            if (needsSuccessFile()) {
                Path markerPath = new Path(jobPath, FileOutputCommitter.SUCCEEDED_FILE_NAME);
                fs.create(markerPath, true).close();
            }

            bulk.deletePrefix(ObjectLocation.pathToObjectLocation(null, new Path(jobPath, "_temporary")));

            if (hasChanges(jobBranch)) {
                CommitsApi commits = lakeFSClient.getCommits();
                Commit c =
                    commits.commit(repository, jobBranch,
                                   new CommitCreation().message(String.format("Add success marker for job %s", jobContext.getJobID())),
                                   null);
                LOG.info("Committed {}", c);
            } else {
                LOG.info("Nothing to commit to {}", jobBranch);
            }

            if (!hasDiffs(repository, jobBranch, outputBranch)) {
                LOG.info("No differences from {} to {}, nothing to merge", jobBranch, outputBranch);
                return;
            }
            LOG.info("Merge job branch {} into {}", jobBranch, outputBranch);
            RefsApi refs = lakeFSClient.getRefs();
            refs.mergeIntoBranch(repository, jobBranch, outputBranch, new Merge().message("").strategy("source-wins"));
        } catch (ApiException e) {
            throw new IOException(String.format("commitJob %s failed", jobBranch), e);
        }
        try {
            cleanupJob();
        } catch (IOException e) {
            LOG.warn("Failed to delete job branch {} after merge (keep going)", jobBranch, e);
        }

    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State status) throws IOException { // TODO(lynn)
        if (outputPath == null)
            return;
        cleanupJob();
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext)
        throws IOException {    // TODO(lynn)
        return true;
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {}

    @Override
    public void commitTask(TaskAttemptContext taskContext)
        throws IOException {
        if (outputPath == null)
            return;
        try {
            LOG.info("Move task {} files from {} to {}", taskID, workPath, jobPath);
            bulk.copyPrefix(ObjectLocation.pathToObjectLocation(workPath),
                            ObjectLocation.pathToObjectLocation(jobPath));
        } catch (ApiException e) {
            throw new IOException(String.format("commitTask %s failed", taskContext.getTaskAttemptID()), e);
        }
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {}

    // TODO(lynn): More methods: isRecoverySupported, isCommitJobRepeatable, recoverTask.
}
