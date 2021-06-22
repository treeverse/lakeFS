package io.treeverse.clients

import com.google.protobuf.timestamp.Timestamp
import io.treeverse.clients.LakeFSContext.{
  LAKEFS_CONF_API_ACCESS_KEY_KEY,
  LAKEFS_CONF_API_SECRET_KEY_KEY,
  LAKEFS_CONF_API_URL_KEY
}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

import scala.annotation.tailrec

object GarbageCollector {

  case class APIConfigurations(apiURL: String, accessKey: String, secretKey: String)

  def getCommitsDF(runID: String, commitDFLocation: String, spark: SparkSession): Dataset[Row] = {
    spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv(commitDFLocation)
      .where(col("run_id") === runID)
  }

  private def getRangeTuples(
      commitID: String,
      repo: String,
      conf: APIConfigurations
  ): Set[(String, String, String)] = {
    val location =
      new ApiClient(conf.apiURL, conf.accessKey, conf.secretKey).getMetaRangeURL(repo, commitID)
    SSTableReader
      .forMetaRange(new Configuration(), location)
      .newIterator()
      .map(range =>
        (new String(range.id), range.message.minKey.toStringUtf8, range.message.maxKey.toStringUtf8)
      )
      .toSet
  }

  def getRangesDFFromCommits(
      commits: Dataset[Row],
      repo: String,
      conf: APIConfigurations
  ): Dataset[Row] = {
    val get_range_tuples = udf((commitID: String) => {
      getRangeTuples(commitID, repo, conf).toSeq
    })

    commits.distinct
      .select(col("expired"), explode(get_range_tuples(col("commit_id"))).as("range_data"))
      .select(
        col("expired"),
        col("range_data._1").as("range_id"),
        col("range_data._2").as("min_key"),
        col("range_data._3").as("max_key")
      )
      .distinct
  }

  def getRangeAddresses(rangeID: String, conf: APIConfigurations, repo: String): Set[String] = {
    val location =
      new ApiClient(conf.apiURL, conf.accessKey, conf.secretKey).getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(new Configuration(), location)
      .newIterator()
      .map(a => new String(a.key))
      .toSet
  }

  def getEntryTuples(
      rangeID: String,
      conf: APIConfigurations
  ): Set[(String, String, Boolean, Long)] = {
    def getSeconds(ts: Option[Timestamp]): Long = {
      ts.getOrElse(0).asInstanceOf[Timestamp].seconds
    }

    val location =
      new ApiClient(conf.apiURL, conf.accessKey, conf.secretKey).getRangeURL("test", rangeID)
    SSTableReader
      .forRange(new Configuration(), location)
      .newIterator()
      .map(a =>
        (
          new String(a.key),
          new String(a.message.address),
          a.message.addressType.isRelative,
          getSeconds(a.message.lastModified)
        )
      )
      .toSet
  }
  def getAddressesTuples(
      rangeIDs: Set[String],
      conf: APIConfigurations
  ): Set[(String, String, Boolean, Long)] = {
    if (rangeIDs.isEmpty) Set()
    else
      getEntryTuples(rangeIDs.head, conf).union(getAddressesTuples(rangeIDs.tail, conf))
  }

  def subtractAddressesInRange(
      addresses: Set[(String, String, Boolean, Long)],
      rangeID: String,
      conf: APIConfigurations,
      repo: String
  ): Set[(String, String, Boolean, Long)] = {
    val activeAddresses = getRangeAddresses(rangeID, conf, repo)
    addresses.filterNot(x => activeAddresses.contains(x._1))
  }

  /** @param leftRangeIDs
   *  @param rightRangeIDs
   *  @param conf
   *  @return tuples of type (key, address, isRelative, lastModified) for every address existing in leftRanges and not in rightRanges
   */
  def leftAntiJoinAddresses(
      leftRangeIDs: Set[String],
      rightRangeIDs: Set[String],
      conf: APIConfigurations,
      repo: String
  ): Set[(String, String, Boolean, Long)] = {
    @tailrec
    def subtractAddressesInRanges(
        addresses: Set[(String, String, Boolean, Long)],
        rangeIDs: Set[String]
    ): Set[(String, String, Boolean, Long)] = {
      if (rangeIDs.isEmpty) addresses
      else
        subtractAddressesInRanges(
          subtractAddressesInRange(addresses, rangeIDs.head, conf, repo),
          rangeIDs.tail
        )
    }
    subtractAddressesInRanges(getAddressesTuples(leftRangeIDs, conf), rightRangeIDs)
  }

  /** receives a dataframe containing active and expired ranges and returns addresses contained only in expired ranges
   *  @param ranges dataframe of type   rangeID:String | expired: Boolean
   *  @return dataframe of type  key:String | address:String | relative:Boolean | last_modified:Long
   */
  def getAddressesDFFromRanges(
      ranges: Dataset[Row],
      conf: APIConfigurations,
      repo: String
  ): Dataset[Row] = {
    val left_anti_join_addresses = udf((x: Seq[String], y: Seq[String]) => {
      leftAntiJoinAddresses(x.toSet, y.toSet, conf, repo).toSeq
    })
    val expiredRangesDF = ranges.where("expired")
    val activeRangesDF = ranges.where("!expired")

    // ranges existing in expired and not in active
    val uniqueExpiredRangesDF = expiredRangesDF.join(
      activeRangesDF,
      expiredRangesDF("range_id") === activeRangesDF("range_id"),
      "leftanti"
    )

    // for every expired range - get all intersecting active ranges
    //  expired_range | active_ranges  | min-max (of expired_range)
    val joinActiveByRange = uniqueExpiredRangesDF
      .as("u")
      .join(
        activeRangesDF.as("a"),
        (col("a.max_key") <= col("u.max_key")) && (col("a.max_key") >= col("u.min_key")) ||
          ((col("a.min_key") <= col("u.max_key")) && (col("a.min_Key") >= col("u.min_key"))),
        "left"
      )
      .select(
        col("u.rangeID").as("unique_range"),
        col("a.rangeID").as("active_range"),
        functions.concat(col("u.min_key"), col("u.max_key")).as("min-max")
      )

    // group unique_ranges and active_ranges by min-max
    // unique_ranges: Set[String] | active_ranges: Set[String] | min-max
    // for each row, unique_ranges contains ranges with exact min-max, active_ranges contain ranges intersecting with min-max
    val groupByMinMax = joinActiveByRange
      .groupBy("min-max")
      .agg(
        collect_set("unique_range").alias("unique_ranges"),
        collect_set("active_range").alias("active_ranges")
      )

    // add a column of all addresses contained only in unique ranges
    // for every Row - [ collection of unique ranges | collection of active ranges] the column would contain a collection of addresses existing only in unique ranges
    val addresses = groupByMinMax.withColumn(
      "addresses",
      left_anti_join_addresses(col("unique_ranges"), col("active_ranges"))
    )

    addresses
      .select(explode(col("addresses")).as("addresses"))
      .select(
        col("addresses._1").as("key"),
        col("addresses._2").as("address"),
        col("addresses._3").as("relative"),
        col("addresses._4").as("last_modified")
      )
  }

  def getExpiredAddresses(
      repo: String,
      runID: String,
      commitDFLocation: String,
      spark: SparkSession,
      conf: APIConfigurations
  ): Dataset[Row] = {
    val commitsDF = getCommitsDF(runID, commitDFLocation, spark)
    val rangesDF = getRangesDFFromCommits(commitsDF, repo, conf)
    getAddressesDFFromRanges(rangesDF, conf, repo)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().getOrCreate()
    if (args.length != 4) {
      Console.err.println(
        "Usage: ... <repo_name> <runID> s3://storageNamespace/prepared_commits_table s3://storageNamespace/output_destination_table"
      )
      System.exit(1)
    }

    val repo = args(0)
    val runID = args(1)
    val commitDFLocation = args(2)
    val addressesDFLocation = args(3)

    val hc = spark.sparkContext.hadoopConfiguration
    val apiURL = hc.get(LAKEFS_CONF_API_URL_KEY)
    val accessKey = hc.get(LAKEFS_CONF_API_ACCESS_KEY_KEY)
    val secretKey = hc.get(LAKEFS_CONF_API_SECRET_KEY_KEY)
    val expiredAddresses = getExpiredAddresses(repo,
                                               runID,
                                               commitDFLocation,
                                               spark,
                                               APIConfigurations(apiURL, accessKey, secretKey)
                                              ).withColumn("run_id", lit(runID))
    expiredAddresses.write.partitionBy("run_id").mode(SaveMode.Append).parquet(addressesDFLocation)
  }
}
