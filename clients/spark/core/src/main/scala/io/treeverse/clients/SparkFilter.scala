package io.treeverse.clients

// SparkFilter is a filter that handles Spark SUCCESS files
class SparkFilter extends KeyFilter {
  // spark success files must end with "/_SUCCESS", unless they are in the root
  // dir, then they should be equal to "_SUCCESS".
  final private val sparkSuccessFileRegex = "(/|^)_SUCCESS$".r

  override def rounds(): Int = 2

  override def shouldHandleKey(key: String, round:  Int): Boolean = {
    val isMatch = sparkSuccessFileRegex.findFirstIn(key) != None

    round match{
      case 1 =>  !isMatch
      case 2 => isMatch
      case default => throw new IllegalArgumentException("SparkSuccessFilter has 2 rounds only")
    }
  }
}
