package io.treeverse.utils

// Goo almost implements Foo, but uses the wrong types....
class Goo {
  def foo(x: Double): Double = x * x

  def another(): Goo = null
}
