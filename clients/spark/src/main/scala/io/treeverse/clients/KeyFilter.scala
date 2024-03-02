package io.treeverse.clients

trait KeyFilter extends Serializable {
  // Number of rounds for this matcher.
  // Rounds are counted from 1, allowing filter logic could be changed based on the number of rounds.
  // For example, export all non-success file on the first round,
  // and all success files on the seconds round.
  def rounds(): Int

  // Return the matching round for the key
  def roundForKey(key: String): Int
}
