// Standalone reader for SSTables.

// Usage from SBT: "lakefs-spark-client-312-hadoop3/test:run /path/to/table.sst"

package io.treeverse.clients

import io.treeverse.lakefs.catalog.Entry
import org.scalameter.api._
import org.scalameter.picklers.Implicits._

object ReadSSTableBenchmark extends Bench.LocalTime {
  val filename = "/tmp/range.sst" // BUG(ariels): Needs to come from the repo!

  val sstables = Gen.single("file")(filename)

  performance of "SSTableReaderIterator" in {
    measure method "next" in {
      using (sstables) in {
        sst => val reader: SSTableReader[Entry] = new SSTableReader(sst, Entry.messageCompanion)
        val count = reader.newIterator.length
      }
    }
  }
}
