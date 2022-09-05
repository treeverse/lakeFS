package io.treeverse.clients

import java.net.URI

import org.scalatest._
import matchers.should._
import funspec._

class ApiClientSpec extends AnyFunSpec with Matchers {
  describe("translateURI") {
    describe("s3") {
      val translate = (u: String) => ApiClient.translateURI(new URI(u), StorageUtils.StorageTypeS3).toString
      it("should translate to s3a") {
        translate("s3://bucket/path/to/object") should be("s3a://bucket/path/to/object")
      }

      it("should translate unadorned bucket to a bucket with no slashes") {
        translate("s3://bucket") should be("s3a://bucket")
      }

      it("should translate unadorned bucket with a slash to a bucket with a slash") {
        translate("s3://bucket/") should be("s3a://bucket/")
      }

      it("should translate bucket with path ending in a slash to a bucket with a path ending in a slash") {
        translate("s3://bucket/path/") should be("s3a://bucket/path/")
      }

      it("should preserve multiple slashes") {
        translate("s3://two//three///") should be("s3a://two//three///")
      }
    }

    describe("Azure") {
      val translate = (u: String) => ApiClient.translateURI(new URI(u), StorageUtils.StorageTypeAzure).toString
      it("should translate any protocol to abfs") {
        translate("https://account.example.net/container/path/to/blob") should be("abfs://container@account.dfs.core.windows.net/path/to/blob")
        translate("ex://account.example.net/container/path/to/blob") should be("abfs://container@account.dfs.core.windows.net/path/to/blob")
      }

      it("should handle empty paths") {
        // TODO(lynn): Trailing slashes might well be incorrect here.
        translate("https://account.example.net/container/") should be("abfs://container@account.dfs.core.windows.net/")
        translate("https://account.example.net/container") should be("abfs://container@account.dfs.core.windows.net/")
      }
    }
  }
}
