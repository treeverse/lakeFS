package io.treeverse.utils

import org.objectweb.asm.ClassVisitor
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type

private object AsInterface {
  def getName(typ: Type): String =
    typ.getSort match {
      case Type.OBJECT => typ.getInternalName
      case Type.ARRAY  => s"Array[${typ.getInternalName}]"
      case _           => typ.getClassName
    }

  def undot(dotted: String) = dotted.replaceAll("\\.", "/")
}

/** ASM ClassVisitor that translates the visited class from the loaded
 *  library into a parallel class that uses `interface` types instead of
 *  exporting its own classes.  When visited, writes a similar class, except
 *  that any type named as a key in ifaces is translated to the corresponding
 *  interface.
 *
 *  This is not always possible and does not even always make sense; here is
 *  what it <em>does</em> do:
 *
 *  1. A class whose name is a key in ifaces is exported from the loaded
 *    library to the client side.  So its matching interface is added to the
 *    list of implements that it implements.
 *  1. Any return type appearing in ifaces is automatically translated to
 *    return as an interface.  While this is not required to compile regular
 *    uses, it _will_ matter when client code tries to select a desired
 *    method.
 *  1. For any interface appearing in ifaces, an overload is generated into
 *    the class.  The overload accepts the interface type(s) and downcasts
 *    them to the required parameter types, then calls the original method
 *    (which might be defined in this class or in a superclass).  This cast
 *    is safe ''as long as the client does not implement the interface on
 *    its own and tries to pass it in''.
 *  1. In particular, any types parametric in ifaces are '''not'''
 *    translated, as there is no way to ensure type safety.  So even if you
 *    define a mapping from a type named `Concrete` to an interface `Iface`
 *    in ifaces, the generated class '''cannot''' handle types such as
 *    `Concrete[]` or `Set<Concrete>`.
 *
 *  Many errors cannot be detected during translation (without reading and
 *  loading the entire class), so will only be detected when the method is
 *  used.  This *is* part of a ClassLoader, and ClassLoaders can have trouble
 *  when translation fails.
 *
 *  @param cv ClassVisitor to use to generate code.
 *  @param ifaces Map of concrete class names in the loaded library to
 *     interface types that will be exposed on the client side.
 */
private class AsInterface(cv: ClassVisitor, val ifaces: Map[String, Class[_]])
    extends ClassVisitor(ASM8, cv) {
  var className: String = ""
  val revIfaces = ifaces.map({ case (iface, klass) => (AsInterface.undot(klass.getName), iface) })
  require(revIfaces.size == ifaces.size, "ifaces map is not 1-to-1")

  override def visit(
      version: Int,
      access: Int,
      name: String,
      signature: String,
      superName: String,
      interfaces: Array[String]
  ) = {
    val (maybeIface, newInterfaces) = ifaces get name match {
      // If this class should be translated to an interface, add a claim
      // that this class implements the interface.
      case Some(iface) => (Some(iface), interfaces :+ Type.getType(iface).getInternalName)
      case None        => (None, interfaces)
    }
    className = name
    cv.visit(version, access, name, signature, superName, newInterfaces)

    maybeIface match {
      case Some(iface) => {
        // Generate wrappers for all public methods of iface in the class.
        iface.getMethods.foreach(method =>
          generateForwardingMethod(ACC_PUBLIC | ACC_SYNTHETIC,
                                   method.getName,
                                   Type.getMethodDescriptor(method),
                                   method.getExceptionTypes.map(e => Type.getType(e).getDescriptor)
                                  )
        )
        // Generate wrapper for all public constructors for the class.
        iface.getConstructors.foreach(ctor =>
          generateForwardingMethod(ACC_PUBLIC | ACC_SYNTHETIC,
                                   "<init>",
                                   Type.getConstructorDescriptor(ctor),
                                   ctor.getExceptionTypes.map(e => Type.getType(e).getDescriptor)
                                  )
        )
      }
      case _ => Unit
    }
  }

  def translateType(typ: Type) =
    ifaces get AsInterface.undot(typ.getInternalName) match {
      case Some(iface) => Type.getType(iface)
      case None        => typ
    }

  def revTranslateType(typ: Type) =
    revIfaces get typ.getInternalName match {
      case Some(className) => Type.getObjectType(className)
      case None            => typ
    }

  def generateForwardingMethod(
      access: Int,
      name: String,
      translatedDesc: String,
      translatedExceptions: Seq[String]
  ): Unit = {
    val translatedArgTypes = Type.getArgumentTypes(translatedDesc)
    if (translatedArgTypes == null) {
      throw new IllegalArgumentException(s"Bad translated method descriptor ${translatedDesc}")
    }
    val argTypes = translatedArgTypes.map(revTranslateType)

    val translatedRetType = Type.getReturnType(translatedDesc)
    val retType = revTranslateType(translatedRetType)

    // BUG(ariels): To handle exceptions, need to wrap INVOKE with
    //
    //     try {...} catch (exception) { throw translatedException }
    val exceptions =
      if (translatedExceptions != null)
        translatedExceptions.map((name: String) =>
          revTranslateType(Type.getObjectType(name)).getInternalName
        )
      else
        null

    val desc = Type.getMethodDescriptor(retType, argTypes: _*)

    if (
      retType.equals(translatedRetType) &&
      (exceptions == null || exceptions.sameElements(translatedExceptions)) &&
      argTypes.sameElements(translatedArgTypes)
    ) {
      return
    }

    val mv = cv.visitMethod(
      access,
      name,
      translatedDesc,
      null /* no support for generics */,
      translatedExceptions.toArray
    )

    mv.visitCode()
    // BUG(ariels): assumes non-static.
    mv.visitVarInsn(ALOAD, 0) // load "this"

    for (((argType, translatedArgType), index) <- (argTypes zip translatedArgTypes).zipWithIndex) {
      mv.visitVarInsn(argType.getOpcode(ILOAD), index + 1) // load arg
      if (!argType.equals(translatedRetType)) {
        // Cast to the desired type.  If the cast fails, throws
        // ClassCastException, which is exactly correct.
        mv.visitTypeInsn(CHECKCAST, argType.getInternalName)
      }
    }

    mv.visitMethodInsn(INVOKESPECIAL, className, name, desc)
    mv.visitInsn(translatedRetType.getOpcode(IRETURN))

    mv.visitMaxs(argTypes.length + 1, argTypes.length + 1)

    mv.visitEnd()
  }
}
