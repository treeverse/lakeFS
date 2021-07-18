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

  // Scala array equality is too hard: the best (only-ish) way is to use
  // java.util.Arrays.equals, but that doesn't work because the variance
  // between Array[A] and Array[Object] is not defined there.
  def equals[T](a: Seq[T], b: Seq[T]): Boolean =
    (a zip b).foldLeft(true) { case (e, (x, y)) => e && x == y }
}

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
      case Some(iface) => (Some(iface), interfaces :+ Type.getType(iface).getInternalName)
      case None        => (None, interfaces)
    }
    className = name
    cv.visit(version, access, name, signature, superName, newInterfaces)

    maybeIface match {
      case Some(iface) => {
        // Generate wrappers for all methods of iface in the class.
        iface.getMethods.foreach(method =>
          generateForwardingMethod(ACC_PUBLIC | ACC_SYNTHETIC,
                                   method.getName,
                                   Type.getMethodDescriptor(method),
                                   method.getExceptionTypes.map(e => Type.getType(e).getDescriptor)
                                  )
        )
        // Generate wrapper for all constructors for the class.
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
      case Some(klassName) => Type.getObjectType(klassName)
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
      (exceptions == null || AsInterface.equals(exceptions, translatedExceptions)) &&
      AsInterface.equals(argTypes, translatedArgTypes)
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
        mv.visitTypeInsn(CHECKCAST, argType.getInternalName)
      }
    }

    mv.visitMethodInsn(INVOKESPECIAL, className, name, desc)
    mv.visitInsn(translatedRetType.getOpcode(IRETURN))

    mv.visitMaxs(argTypes.length + 1, argTypes.length + 1)

    mv.visitEnd()
  }
}
