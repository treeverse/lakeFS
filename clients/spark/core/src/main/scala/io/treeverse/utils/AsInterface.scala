package io.treeverse.utils

import org.objectweb.asm.{ClassVisitor, MethodVisitor}
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.Type

import java.lang.reflect.Method

private object AsInterface {
  def getName(typ: Type): String =
    typ.getSort match {
      case Type.OBJECT => typ.getInternalName
      case Type.ARRAY  => s"Array[${typ.getInternalName}]"
      case _           => typ.getClassName
    }

  def loadFor(sort: Int) = {
    import Type._
    sort match {
      case BOOLEAN | CHAR | SHORT | INT => ILOAD
      case LONG                         => LLOAD
      case FLOAT                        => FLOAD
      case DOUBLE                       => DLOAD
      case ARRAY | OBJECT               => ALOAD
      case _ => throw new IllegalArgumentException(s"cannot load type of sort ${sort}")
    }
  }

  def returnFor(sort: Int) = {
    import Type._
    sort match {
      case BOOLEAN | CHAR | SHORT | INT => IRETURN
      case LONG                         => LRETURN
      case FLOAT                        => FRETURN
      case DOUBLE                       => DRETURN
      case ARRAY | OBJECT               => ARETURN
      case VOID                         => RETURN
      case _ => throw new IllegalArgumentException(s"cannot load type of sort ${sort}")
    }
  }

  // Scala array equality is too hard: the best (only-ish) way is to use
  // java.util.Arrays.equals, but that doesn't work because the variance
  // between Array[A] and Array[Object] is not defined there.
  def equals[T](a: Array[T], b: Array[T]): Boolean =
    (a zip b).foldLeft(true) { case (e, (x, y)) => e && x == y }
}

private class AsInterface(cv: ClassVisitor, val ifaces: Map[String, Class[_]])
    extends ClassVisitor(ASM8, cv) {
  var className: String = ""
  // If set, the interface to be translated.
  var toIface: Class[_] = null
  var translatedMethodDescs = Set[String]()

  override def visit(
      version: Int,
      access: Int,
      name: String,
      signature: String,
      superName: String,
      interfaces: Array[String]
  ) = {
    val newInterfaces = ifaces get name match {
      case Some(iface) => {
        toIface = iface
        interfaces :+ Type.getType(iface).getInternalName
      }
      case None => interfaces
    }
    className = name
    cv.visit(version, access, name, signature, superName, newInterfaces)
  }

  def translateType(typ: Type) =
    ifaces get typ.getInternalName.replaceAll("\\.", "/") match {
      case Some(iface) => Type.getType(iface)
      case None        => typ
    }

  // Verify that any interface methods were translated, i.e. implemented.
  override def visitEnd(): Unit = {
    if (toIface == null) {
      // No translation, nothing to verify.
      return
    }
    val interfaceMethodDescs = toIface.getDeclaredMethods
      .map((m: Method) => m.getName + ": " + Type.getMethodDescriptor(m))
      .toSet
    val unimplemented = interfaceMethodDescs -- translatedMethodDescs
    if (!unimplemented.isEmpty) {
      throw new UnimplementedMethodsException("Unimplemented methods", unimplemented)
    }
  }

  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      signature: String,
      exceptions: Array[String]
  ): MethodVisitor = {
    // BUG(ariels): Does not support generic methods (ignores signature).
    //     At least throw an exception...

    // BUG(ariels): Test exceptions.
    generateForwardingMethod(access, name, desc, exceptions)

    val mv = cv.visitMethod(access, name, desc, null, exceptions)
    return mv;
  }

  def generateForwardingMethod(
      access: Int,
      name: String,
      desc: String,
      exceptions: Array[String]
  ): Unit = {
    val argTypes = Type.getArgumentTypes(desc)
    if (argTypes == null) throw new IllegalArgumentException(s"Bad method descriptor ${desc}")
    val translatedArgTypes = argTypes.map(translateType)

    val retType = Type.getReturnType(desc)

    val translatedRetType = translateType(retType)

    val translatedExceptions =
      if (exceptions != null)
        exceptions.map((name: String) => translateType(Type.getObjectType(name)).getInternalName)
      else
        exceptions

    val translatedDesc = Type.getMethodDescriptor(translatedRetType, translatedArgTypes: _*)

    translatedMethodDescs = translatedMethodDescs + (name + ": " + translatedDesc)

    if (
      retType.equals(translatedRetType) &&
      (exceptions == null || AsInterface.equals(exceptions, translatedExceptions)) &&
      AsInterface.equals(argTypes, translatedArgTypes)
    ) {
      return
    }

    val mv = cv.visitMethod(access,
                            name,
                            translatedDesc,
                            null /* no support for generics */,
                            translatedExceptions
                           )

    mv.visitCode()
    // BUG(ariels): assumes non-static.
    mv.visitVarInsn(ALOAD, 0) // load "this"

    for (((argType, translatedArgType), index) <- (argTypes zip translatedArgTypes).zipWithIndex) {
      mv.visitVarInsn(AsInterface.loadFor(argType.getSort), index + 1) // load arg
      if (!argType.equals(translatedRetType)) {
        mv.visitTypeInsn(CHECKCAST, argType.getInternalName)
      }
    }

    mv.visitMethodInsn(INVOKESPECIAL, className, name, desc)
    mv.visitInsn(AsInterface.returnFor(translatedRetType.getSort))

    mv.visitMaxs(argTypes.length + 1, argTypes.length + 1)

    mv.visitEnd()
  }
}

private class UnimplementedMethodsException(message: String, val methods: Set[String])
    extends ClassCastException(s"${message}: ${methods.iterator.mkString(", ")}") {}
