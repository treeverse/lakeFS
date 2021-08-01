package io.treeverse.utils

import org.objectweb.asm.{ClassReader, ClassWriter}
import org.objectweb.asm.util.{CheckClassAdapter, TraceClassVisitor}
import java.io.{ByteArrayOutputStream, PrintWriter}
import com.google.common.io.ByteStreams

class AsInterfaceClassLoader(
    parent: ClassLoader,
    ifaces: Map[String, Class[_]],
    val pw: PrintWriter = null
) extends ClassLoader(parent) {

  // Some additional classes never to attempt to translate.  If an interface
  // is supplied that is *also* defined on the library side, cannot
  // translate it -- so don't even try.  The interfaces in particular are
  // used by the classloader, so need to be loaded -- transfer them directly
  // to the parent classloader.
  val skip = ifaces.values.map(_.getName).toSet

  def newInstance[C](name: String, args: Any*): C = {
    val argsTypes = args.map(_.getClass).toArray
    val ctor = loadClass(name).getConstructor(argsTypes: _*)
    // Box any primitives so they become Objects for the newInstance call.
    val boxedArgs = args.map(_.asInstanceOf[AnyRef])
    ctor.newInstance(boxedArgs: _*).asInstanceOf[C]
  }

  private def asInterfaceClass(bytes: Array[Byte]): Array[Byte] = {
    val cr = new ClassReader(bytes)
    val cw = new ClassWriter(cr, 0)
    val tw = if (pw != null) new TraceClassVisitor(cw, pw) else cw
    val cc = new CheckClassAdapter(tw)
    // TODO(ariels): Add a visitor to verify that resulting class satisfies
    //     the interface from ifaces (if any).  Otherwise methods are only
    //     checked when (if) used.
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
