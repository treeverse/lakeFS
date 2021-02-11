package io.treeverse.clients

import io.treeverse.clients.committed.RangeData

class Range(val key: Array[Byte], val identity: Array[Byte], val data: RangeData)
