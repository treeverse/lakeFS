package io.treeverse.clients

// SparkFilter is a filter that handles Spark SUCCESS files
class SparkFilter extends KeyFilter {
  // spark success files must end with "/_SUCCESS", unless they are in the root
  // dir, then they should be equal to "_SUCCESS".
  final private val sparkSuccessFileRegex = "(.*/|^)_SUCCESS$".r

  override def rounds(): Int = 2

  override def roundForKey(key: String): Int = {
    key match {
      case sparkSuccessFileRegex(_) => 2
      case _                        => 1
    }
  }
}
