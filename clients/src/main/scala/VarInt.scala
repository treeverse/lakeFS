import java.io.DataInput
import java.io.IOException


/**
 * from https://github.com/addthis/stream-lib/blob/master/src/main/java/com/clearspring/analytics/util/Varint.java
 */
object VarInt {
  @throws[IOException]
  def readSignedVarLong(in: DataInput): Long = {
    val raw = readUnsignedVarLong(in)
    // This undoes the trick in writeSignedVarLong()
    val temp = (((raw << 63) >> 63) ^ raw) >> 1
    // This extra step lets us deal with the largest signed values by treating
    // negative results from read unsigned methods as like unsigned values
    // Must re-flip the top bit if the original read value had it set.
    temp ^ (raw & (1L << 63))
  }

  @throws[IOException]
  def readUnsignedVarLong(in: DataInput): Long = {
    var value = 0L
    var i = 0
    var b = 0L

    while ( {
      b = in.readByte()
      (b & 0x80L) != 0
    }) {
      value |= (b & 0x7F) << i
      i += 7
      if (i > 63) throw new IllegalArgumentException("Variable length quantity is too long")
    }
    value | (b << i)
  }
}
