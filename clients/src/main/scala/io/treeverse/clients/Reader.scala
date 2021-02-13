package io.treeverse.clients

import com.google.protobuf.Message

trait ProtoReader[Proto <: Message] {
  def get(ID : Array[Byte]) : Seq[EntryRecord[Proto]]
}
