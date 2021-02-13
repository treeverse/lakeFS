package io.treeverse.clients

import com.google.protobuf.Message

class EntryRecord[Proto <: Message](val key: Array[Byte], val identity: Array[Byte], val value: Proto)
