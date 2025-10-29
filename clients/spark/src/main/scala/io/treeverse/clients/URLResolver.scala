package io.treeverse.clients

import java.net.URI
import org.apache.hadoop.fs.Path

object URLResolver extends Serializable {

  /** resolves a url given the path.
   *  If the path is a full url to the object store return that.
   *  Else, concatenates the path to the url.
   */
  def resolveURL(uri: URI, path: String): Path = {
    if (path.contains("://")) {
      return new Path(path)
    }

    // uri should end with a trailing '/'
    var uriPath = uri.toString
    uriPath = if (uriPath.endsWith("/")) uriPath else uriPath.concat("/")

    // path should not start with a leading '/'
    val extraPath = if (path.startsWith("/")) path.substring(1) else path

    new Path(uriPath + extraPath)
  }
}
