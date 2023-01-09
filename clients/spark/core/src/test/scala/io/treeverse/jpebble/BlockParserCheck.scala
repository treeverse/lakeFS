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
  val makeLE32 = (x: Int) => Seq(x, x >>> 8, x >>> 16, x >>> 24).map(_.toByte).iterator

  describe("readInt32") {
    it("reads little-endian int32s") {
      forAll { (a: Int) => BlockParser.readInt32(makeLE32(a)) should be(a) }
    }

    it("equals readFixedInt") {
      val gen = Gen.containerOfN[Seq, Byte](4, arbByte.arbitrary)
      forAll(gen) {
        case (a) => BlockParser.readInt32(a.iterator) should be(BlockParser.readFixedInt(a.iterator))
      }
    }
  }
}
