package io.treeverse.clients

trait RangeReader {
  def getEntries(rangeID : Array[Byte]) : Seq[EntryRecord]
}
