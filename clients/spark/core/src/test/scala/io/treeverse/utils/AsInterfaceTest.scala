package io.treeverse.utils

import org.scalatest.funsuite.AnyFunSuite

import org.objectweb.asm.{ClassReader, ClassWriter}
import org.objectweb.asm.util.CheckClassAdapter

import java.io.ByteArrayOutputStream
import com.google.common.io.ByteStreams

class AsInterfaceTest extends AnyFunSuite {
  test("[internal] Boo does not implement Foo") {
    assertThrows[ClassCastException] {
      (new Boo(9)).asInstanceOf[Foo]
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
    val f = aicl.newInstance[Foo]("io.treeverse.utils.Boo", 9)
    assert(f.foo(4) === 36)
  }

  test("Boo as Foo can use method taking and returning a Boo") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Boo", classOf[Foo])))
    val f: Foo = aicl.newInstance[Foo]("io.treeverse.utils.Boo", 9)
    val f2: Foo = aicl.newInstance[Foo]("io.treeverse.utils.Boo", 3)
    assert(f.addFoo(f2).foo(4) === (9 + 3) * 4)
  }

  test("Goo as Foo fails to call missing method at call time :-(") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Goo", classOf[Foo])))
    val notF: Foo = aicl.newInstance[Foo]("io.treeverse.utils.Goo")
    assertThrows[AbstractMethodError](notF.foo(0))
  }

  test("Boo as Foo is not a Moo") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Boo", classOf[Foo])))
    assertThrows[ClassCastException](
      aicl.newInstance[Moo]("io.treeverse.utils.Boo", 9)
    )
  }

  test("Boo as a Moo fails to call missing method at call time :-(") {
    val aicl = new AsInterfaceClassLoader(
      getClass.getClassLoader,
      Map(("io/treeverse/utils/Boo", classOf[Moo])))
    val notM: Moo = aicl.newInstance[Moo]("io.treeverse.utils.Boo", 9)
    assertThrows[AbstractMethodError](notM.bar(0))
  }
}
