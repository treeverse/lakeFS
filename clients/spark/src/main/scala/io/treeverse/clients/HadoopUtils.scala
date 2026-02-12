package io.treeverse.clients

import org.apache.hadoop.conf.Configuration
import scala.jdk.CollectionConverters._

object HadoopUtils {
  def getHadoopConfigurationValues(hc: Configuration, prefixes: String*) =
    hc.iterator.asScala
      .filter(c => prefixes.exists(c.getKey.startsWith))
      .map(entry => (entry.getKey, entry.getValue))
      .toArray
}
