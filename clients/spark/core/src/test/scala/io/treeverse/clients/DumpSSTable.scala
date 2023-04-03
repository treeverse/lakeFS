// Standalone reader for SSTables.

// Usage from SBT: "lakefs-spark-client-312-hadoop3/test:run /path/to/table.sst"

package io.treeverse.clients

import io.treeverse.lakefs.catalog.Entry
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

object ReadSSTableBenchmark extends Bench.LocalTime {
  val filename = "/bench/56569996.sst"
  val sstURL = getClass.getResource(filename)

  val sstables = Gen.single("file")(sstURL.getPath)

  performance of "SSTableReaderIterator" in {
    measure method "next" in {
      using (sstables) in {
        sst => {
          val reader: SSTableReader[Entry] = new SSTableReader(sst, Entry.messageCompanion, true)
          val count = reader.newIterator.length
        }
      }
    }
  }
}
