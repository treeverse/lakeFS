package io.treeverse.clients

trait KeyFilter extends Serializable {
  // Number of rounds for this matcher.
  // Matcher logic could be changed based on the number of rounds.
  // For example, export all non-success file on the first round,
  // and all success files on the seconds round.
  def rounds(): Int

  // Given the key and the round, decide whether the key should be handled.
  def shouldHandleKey(key: String, round: Int) : Boolean
}
