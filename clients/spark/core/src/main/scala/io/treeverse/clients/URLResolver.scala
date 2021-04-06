package io.treeverse.clients

import java.net.URI
import org.apache.hadoop.fs.Path

object URLResolver extends Serializable{

  /** resolves a url which is relative to the full uri
   *
   * @param uri includes schema and may include a path
   * @param extraPath must be a relative url
   * @return the concatenated path
   */
  def resolveURL(uri: URI, extraPath: String): Path = {
    val newPath: String = uri.getPath + '/' + extraPath
    new Path(uri.resolve(newPath))
  }
}
