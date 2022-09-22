package io.treeverse.clients

import io.treeverse.clients.LakeFSContext.LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_EPOCH_SECONDS_KEY
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
trait RangeGetter extends Serializable {

  /** @return all rangeIDs in metarange of commitID on repo.
   */
  def getRangeIDs(commitID: String, repo: String): Iterator[String]

  /** @return all object addresses in range rangeID on repo
   */
  def getRangeAddresses(rangeID: String, repo: String): Iterator[String]
}

class LakeFSRangeGetter(val apiConf: APIConfigurations, val configMapper: ConfigMapper)
    extends RangeGetter {
  override def getRangeIDs(commitID: String, repo: String): Iterator[String] = {
    val conf = configMapper.configuration
    val apiClient = ApiClient.get(apiConf)
    val commit = apiClient.getCommit(repo, commitID)
    val maxCommitEpochSeconds = conf.getLong(LAKEFS_CONF_DEBUG_GC_MAX_COMMIT_EPOCH_SECONDS_KEY, -1)
    if (maxCommitEpochSeconds > 0 && commit.getCreationDate > maxCommitEpochSeconds) {
      return Iterator.empty
    }
    val location = apiClient.getMetaRangeURL(repo, commit)
    // continue on empty location, empty location is a result of a commit
    // with no metaRangeID (e.g 'Repository created' commit)
    if (location == "") Iterator.empty
    else
      SSTableReader
        .forMetaRange(conf, location)
        .newIterator()
        .map(o => new String(o.id))
  }

  override def getRangeAddresses(rangeID: String, repo: String): Iterator[String] = {
    val location = ApiClient
      .get(apiConf)
      .getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(configMapper.configuration, location)
      .newIterator()
      .filter(_.message.addressType.isRelative)
      .map(a => a.message.address)
  }

  def getRangeEntries(rangeID: String, repo: String): Iterator[(String, String)] = {
    val location = ApiClient
      .get(apiConf)
      .getRangeURL(repo, rangeID)
    SSTableReader
      .forRange(configMapper.configuration, location)
      .newIterator()
      .map(a => (a.message.address, new String(a.key)))
  }
}

class LakeFSRangeHelper(
    val rangeGetter: LakeFSRangeGetter
) extends Serializable {
  @transient lazy val spark = SparkSession.active

  /** Returns a dataset with all commit-range pairs.
   */
  def getRangeIDsWithCommitID(
      commitIDs: Dataset[String],
      repo: String
  ): Dataset[(String, String)] = {
    import spark.implicits._
    commitIDs.flatMap(commitID => rangeGetter.getRangeIDs(commitID, repo).map((commitID, _)))
  }

  /** Returns a dataset with all range-address-logical_key tuples.
   */
  def getRangeEntriesWithRangeID(
      rangeIDs: Dataset[String],
      repo: String
  ): Dataset[(String, String, String)] = {
    import spark.implicits._
    rangeIDs.flatMap(rangeID =>
      rangeGetter.getRangeEntries(rangeID, repo).map(e => (rangeID, e._1, e._2))
    )
  }
}
