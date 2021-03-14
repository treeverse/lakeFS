package io.treeverse.clients

import com.google.common.cache.CacheBuilder
import org.json4s._
import org.json4s.native.JsonMethods._
import scalaj.http.Http

import java.net.URI
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.Callable

class ApiClient(apiUrl: String, accessKey: String, secretKey: String) {
  private val storageNamespaceCache = CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.MINUTES).build[String, String]()

  private class CallableFn(val fn: () => String) extends Callable[String] {
    def call(): String = fn()
  }

  private def getStorageNamespace(repoName: String): String = {
    storageNamespaceCache.get(repoName, new CallableFn(() => {
      val getRepositoryURI = URI.create("%s/repositories/%s".format(apiUrl, repoName)).normalize()
      val resp = Http(getRepositoryURI.toString).header("Accept", "application/json").auth(accessKey, secretKey).asString
      val JString(storageNamespace) = parse(resp.body) \ "storage_namespace"
      var snUri = URI.create(storageNamespace)
      snUri = new URI(if (snUri.getScheme == "s3") "s3a" else snUri.getScheme, snUri.getHost, snUri.getPath, snUri.getFragment)
      snUri.normalize().toString
    }))
  }

  def getMetaRangeURL(repoName: String, commitID: String): String = {
    val getCommitURI = URI.create("%s/repositories/%s/commits/%s".format(apiUrl, repoName, commitID)).normalize()
    val commitResp = Http(getCommitURI.toString).header("Accept", "application/json").auth(accessKey, secretKey).asString
    val commit = parse(commitResp.body)
    if (commitResp.isError) {
      throw new RuntimeException(s"failed to get commit ${commitID}: [${commitResp.code}] ${commitResp.body}")
    }
    val metaRangeID = commit \ "meta_range_id" match {
      case JString(metaRangeID) => metaRangeID
      case _ => // TODO(ariels): Bad parse exception type
        throw new RuntimeException(s"expected string meta_range_id in ${commitResp.body}")
    }

    val getMetaRangeURI = URI.create("%s/repositories/%s/metadata/meta_range/%s".format(apiUrl, repoName, metaRangeID)).normalize()
    val metaRangeResp = Http(getMetaRangeURI.toString).header("Accept", "application/json").auth(accessKey, secretKey).asString
    if (metaRangeResp.isError) {
      throw new RuntimeException(s"failed to get meta_range ${metaRangeID}: [${metaRangeResp.code}] ${metaRangeResp.body}")
    }
    val location = metaRangeResp.header("Location") match {
      case Some(l) => l
      case None => throw new RuntimeException("missing Location header in response for meta_range %s".format(metaRangeID))
    }
    URI.create(getStorageNamespace(repoName) + "/" + location).normalize().toString
  }

  def getRangeURL(repoName: String, rangeID: String): String = {
    val getRangeURI = URI.create("%s/repositories/%s/metadata/range/%s".format(apiUrl, repoName, rangeID)).normalize()
    val resp = Http(getRangeURI.toString).header("Accept", "application/json").auth(accessKey, secretKey).asString
    if (resp.isError) {
      throw new RuntimeException(s"failed to get range ${rangeID}: [${resp.code}] ${resp.body}")
    }
    val location = resp.header("Location") match {
      case Some(l) => l
      case None => throw new RuntimeException("missing Location header in response for range %s".format(rangeID))
    }
    URI.create(getStorageNamespace(repoName) + "/" + resp.header("Location").get).normalize().toString
  }
}
