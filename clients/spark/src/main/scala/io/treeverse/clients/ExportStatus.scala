package io.treeverse.clients

/** Export status of a single file
 *
 *  @param path of the file
 *  @param success indicates if the file export succeeded
 *  @param msg additional information of the status
 */
case class ExportStatus(path: String, success: Boolean, msg: String) extends Serializable {}
