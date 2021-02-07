package io.treeverse.clients

trait MetaRangeReader {
  def getRanges(metaRangeID: String) : List[Range]
}
