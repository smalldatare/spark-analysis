package org.richardqiao.scala.test

object ForTest {
  def main(args: Array[String]): Unit = {
    var toGo: Boolean = true
    for (i <- 1 to 10; if toGo) {
      println(i)
      if (i == 6) { toGo = false } else { toGo = true }

    }
  }
}