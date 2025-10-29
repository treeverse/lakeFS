package io.treeverse.clients

import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast

/** Maps a broadcast object including hadoop configuration values into hadoop configuration.
 *  @param hcValues
 */
class ConfigMapper(val hcValues: Broadcast[Array[(String, String)]]) extends Serializable {
  @transient lazy val configuration = {
    val conf = new Configuration()
    hcValues.value.foreach({ case (k, v) => conf.set(k, v) })
    conf
  }
}
