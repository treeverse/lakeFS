package io.treeverse.clients

import java.net.URI
import org.apache.hadoop.fs.Path

object URLResolver extends Serializable{
  def resolveURL(uri: URI, extraPath: String): Path = {
    val newPath: String = uri.getPath + '/' + extraPath
    new Path(uri.resolve(newPath))
  }
}
