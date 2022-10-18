package io.lakefs.committer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.parquet.hadoop.ParquetOutputCommitter;
import org.apache.parquet.hadoop.util.ContextUtil;

import java.io.IOException;

public class DummyParquetOutputCommitter extends DummyOutputCommitter {
    public DummyParquetOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);
    }

    public DummyParquetOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
    }

    public void commitJob(JobContext jobContext) throws IOException {
        super.commitJob(jobContext);
        Configuration configuration = ContextUtil.getConfiguration(jobContext);
        ParquetOutputCommitter.writeMetaDataFile(configuration, outputPath);
    }
}
