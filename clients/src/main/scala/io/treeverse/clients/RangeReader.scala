package io.treeverse.clients

trait RangeReader {
  def getEntries(rangeID : String) : List[EntryRecord]
}
