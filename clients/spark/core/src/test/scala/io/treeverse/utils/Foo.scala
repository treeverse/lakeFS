package io.treeverse.utils

trait Foo {
  def foo(x: Int): Int

  def another(): Foo

  def addFoo(f: Foo): Foo
}
