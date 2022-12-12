package io.treeverse.gc

class GCExceptions(message: String) extends Exception(message) {

  def this(message: String, cause: Throwable) {
    this("GC Failure: " + message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }
}

class FailedRunException(message: String) extends GCExceptions(message) {
  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }
}

class ParameterValidationException(message: String) extends GCExceptions(message) {
  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }
}
