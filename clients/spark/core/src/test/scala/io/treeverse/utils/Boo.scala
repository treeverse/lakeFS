package io.treeverse.utils

// Boo could implement Foo, but doesn't...
class Boo {
  def foo(x: Int): Int = x * x

  def another(): Boo = new Boo
}
