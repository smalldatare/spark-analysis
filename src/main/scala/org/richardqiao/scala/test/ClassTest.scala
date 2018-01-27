package org.richardqiao.scala.test

import scala.reflect.ClassTag

object ClassTest {
  class Person()
  class Student extends Person
  def main(args: Array[String]): Unit = {
    val c = implicitly[ClassTag[Person]].runtimeClass
    println(c)
  }
}