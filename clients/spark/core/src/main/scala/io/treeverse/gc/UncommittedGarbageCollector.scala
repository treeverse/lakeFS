package io.treeverse.gc

object UncommittedGarbageCollector {
  def main(args: Array[String]): Unit = {
    println("This class is deprecated. Please use io.treeverse.gc.GarbageCollector instead.")
    val region = if (args.length == 2) args(1) else null
    val repo = args(0)
    GarbageCollector.run(repo, region, true, "uncommitted_gc")
  }
}
