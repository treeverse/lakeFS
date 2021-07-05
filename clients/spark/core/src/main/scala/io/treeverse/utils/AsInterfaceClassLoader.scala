package io.treeverse.utils

import org.objectweb.asm.{ClassReader, ClassWriter}
import org.objectweb.asm.util.{CheckClassAdapter}
import java.io.ByteArrayOutputStream
import com.google.common.io.ByteStreams

class AsInterfaceClassLoader(
  parent: ClassLoader, ifaces: Map[String, Class[_]]) extends ClassLoader(parent) {

  val skip = ifaces.values.map(_.getName).toSet

  private def asInterfaceClass(bytes: Array[Byte]): Array[Byte] = {
    val cr = new ClassReader(bytes)
    val cw = new ClassWriter(cr, 0)
    val cc = new CheckClassAdapter(cw)
    val ai = new AsInterface(cc, ifaces)

    cr.accept(ai, 0)
    cw.toByteArray
  }

  override protected def loadClass(name: String, resolve: Boolean): Class[_] = {
    if (name.startsWith("java.") || (skip contains name)) {
      // Unsafe to define these classes, no point in trying.
      return getParent.loadClass(name)
    }

    val cls = {
      {
        val loaded = findLoadedClass(name)
        if (loaded != null) {
          return loaded
        }
      }
      {
        val found = findClass(name)
        if (found != null) {
          return found
        }
      }
      return getParent.loadClass(name)
    }
    if (resolve) {
      resolveClass(cls)
    }
    cls
  }

  override protected def findClass(name: String): Class[_] = {
    val internalName = name.replaceAll("\\.", "/") + ".class"
    val classStream = parent.getResourceAsStream(internalName)
    if (classStream == null) {
      throw new ClassNotFoundException(name)
    }
    val rawBytesStream = new ByteArrayOutputStream
    ByteStreams.copy(classStream, rawBytesStream)
    val rawBytes = rawBytesStream.toByteArray
    val bytes = asInterfaceClass(rawBytes)
    defineClass(name, bytes, 0, bytes.length)
  }
}
