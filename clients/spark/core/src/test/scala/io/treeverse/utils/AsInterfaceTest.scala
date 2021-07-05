package io.treeverse.utils

import org.scalatest.funsuite.AnyFunSuite

import org.objectweb.asm.{ClassReader, ClassWriter}
import org.objectweb.asm.util.CheckClassAdapter

import java.io.ByteArrayOutputStream
import com.google.common.io.ByteStreams

class AsInterfaceTest extends AnyFunSuite {
  test("[internal] Boo does not implement Foo") {
    assertThrows[ClassCastException] {
      (new Boo).asInstanceOf[Foo]
    }
  }

  test("AsInterface: Boo as Foo") {
    val booStream = this.getClass.getClassLoader.getResourceAsStream("io/treeverse/utils/Boo.class")
    val booBytes = new ByteArrayOutputStream
    ByteStreams.copy(booStream, booBytes)

    val cr = new ClassReader(booBytes.toByteArray)
    val cw = new ClassWriter(cr, 0)
    val cc = new CheckClassAdapter(cw);
    val ai = new AsInterface(cc, Map(("Foo", classOf[Foo])))
    cr.accept(ai, 0)
  }

  test("Boo as Foo") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Boo", classOf[Foo])))
    val f: Foo = aicl.loadClass("io.treeverse.utils.Boo").newInstance.asInstanceOf[Foo]
    assert(f.foo(4) === 16)
  }

  test("Boo as Foo can use method returning a Boo") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Boo", classOf[Foo])))
    val f: Foo = aicl.loadClass("io.treeverse.utils.Boo").newInstance.asInstanceOf[Foo]
    assert(f.another.foo(4) === 16)
  }

  test("Goo as Foo fails") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Goo", classOf[Foo])))
    val f: Foo = aicl.loadClass("io.treeverse.utils.Goo").newInstance.asInstanceOf[Foo]
    assertThrows[AbstractMethodError](f.foo(4))
  }

  test("Boo as Foo is not a Moo") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Boo", classOf[Foo])))
    assertThrows[ClassCastException](
      aicl.loadClass("io.treeverse.utils.Boo").newInstance.asInstanceOf[Moo]
    )
  }

  test("Boo as a Moo fails at runtime") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Boo", classOf[Moo])))
    assertThrows[InstantiationException](
      aicl.loadClass("io.treeverse.utils.Moo").newInstance.asInstanceOf[Moo])
  }
}
