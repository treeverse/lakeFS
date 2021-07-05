package io.treeverse.utils

import org.objectweb.asm.{ClassVisitor, MethodVisitor}
import org.objectweb.asm.Opcodes.ASM8
import org.objectweb.asm.Type

private class AsInterface(cv: ClassVisitor, val ifaces: Map[String, Class[_]]) extends ClassVisitor(ASM8, cv) {
  override def visit(
    version: Int,
    access: Int,
    name: String,
    signature: String,
    superName: String,
    interfaces: Array[String]
  ) = {
    val newInterfaces = ifaces get name match {
        case Some(iface) => interfaces :+ Type.getType(iface).getInternalName
        case None => interfaces
      }
    cv.visit(version, access, name, signature, superName, newInterfaces)
  }

  val descReturnType = """\)L([a-zA-Z_0-9/]+);""".r
  def translateDesc(desc: String) =
    descReturnType.replaceAllIn(desc, _ match {
      case scala.util.matching.Regex.Groups(name) => /* BUG */ s")L${translateName(name)};"
    })

  def translateName(name: String) =
    ifaces get name match {
      case Some(iface) => iface.getName.replaceAll("\\.", "/")
      case None => name
    }

  override def visitMethod(access: Int, name: String, desc: String, signature: String, exceptions: Array[String]): MethodVisitor = {
    Console.out.println(s"[DEBUG] name ${name} desc ${desc} signature ${signature}")
    val newDesc = translateDesc(desc)
    val mv = cv.visitMethod(access, name, newDesc, signature, exceptions)
    return mv
  }
}
