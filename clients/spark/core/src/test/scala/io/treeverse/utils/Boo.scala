package io.treeverse.utils

// Boo could implement Foo, but doesn't...  
class Boo(val s: Integer) {
  def foo(x: Int): Int = s * x

  def addFoo(b: Boo): Boo = new Boo(s + b.s)

  def another(): Boo = return new Boo(s)
}
