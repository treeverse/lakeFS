// Benchmark standalone reader for SSTables.

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
          val reader: SSTableReader[Entry] = new SSTableReader(sst, Entry.messageCompanion)
          val count = reader.newIterator.length
        }
      }
    }
  }
}
