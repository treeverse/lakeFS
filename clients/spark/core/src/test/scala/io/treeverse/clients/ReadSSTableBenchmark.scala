// Standalone reader for SSTables.

// Usage from SBT: "lakefs-spark-client/test:run /path/to/table.sst"

package io.treeverse.clients

import io.treeverse.lakefs.catalog.Entry
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

object ReadSSTableBenchmark extends Bench.LocalTime {

  performance of "SSTableReaderIterator" in {
    val sstURL = getClass.getResource("/bench/56569996.sst")
    val ssTables = Gen.single("file")(sstURL.getPath)
    measure method "next" in {
      using(ssTables) in { sst =>
        {
          val reader: SSTableReader[Entry] = new SSTableReader(sst, Entry.messageCompanion, false)
          val _ = reader.newIterator().length
        }
      }
    }
  }
}
