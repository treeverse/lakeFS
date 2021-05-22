from pyspark.sql import SparkSession
import pyspark
import sys

spark = SparkSession.builder.appName("test_app").getOrCreate()
spark._jsc.hadoopConfiguration().set("fs.lakefs.access.key", "AKIAIOSFODNN7EXAMPLE")
spark._jsc.hadoopConfiguration().set("fs.lakefs.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
sc = spark.sparkContext
sc.setLogLevel("DEBUG")
log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

# Write using the lakefs file system
samples = sc.parallelize([
    ("abonsanto@fakemail.com", "Alberto", "Bonsanto"),
    ("dbonsanto@fakemail.com", "Dakota", "Bonsanto")
])
# samples.collect()
samples.saveAsTextFile("lakefs://example1/master/output.txt")

# samples.unpersist()

# Read file using the lakefs file system
lines = sc.textFile("lakefs://example1/master/input.txt")
lines.taks(10).foreach(log.trace)

sc.stop()
