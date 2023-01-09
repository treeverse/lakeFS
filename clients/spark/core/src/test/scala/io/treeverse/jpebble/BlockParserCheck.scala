package io.treeverse.jpebble

import org.scalatest._
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen

import matchers.should._
import funspec._

/** Test suite to perform property-based checking on parsers.  See
  * https://github.com/typelevel/scalacheck/blob/main/doc/UserGuide.md.
 */
class BlockParserCheck extends AnyFunSpec with ScalaCheckDrivenPropertyChecks with Matchers {

  describe("readInt32") {
    val makeLE32 = (x: Int) => Seq(x, x >>> 8, x >>> 16, x >>> 24).map(_.toByte).iterator
    it("reads little-endian int32s") {
      forAll { (bytes: Int) => BlockParser.readInt32(makeLE32(bytes)) should be(bytes) }
    }

  }
}
