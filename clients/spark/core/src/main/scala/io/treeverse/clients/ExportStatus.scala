package io.treeverse.clients

// ExportStatus is the export status of a single file
case class ExportStatus(path: String, success: Boolean, msg: String) extends Serializable {}
