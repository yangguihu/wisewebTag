package com.wiseweb.test

import java.io.{File, PrintWriter}

import scala.io.Source

/**
  * Created by yangguihu on 2017/3/6.
  * 生成模拟 公司关心关键词数据
  */
object Companykeys {
  def main(args: Array[String]) {

    val lines = Source.fromFile("data/key2.txt").getLines()
    val set1 = new scala.collection.mutable.HashSet[Int]()
    val printWriter: PrintWriter = new PrintWriter(new File("data/rule1.txt"))
    lines.foreach(line => {
      val random = Math.random() * 20
      for (i <- 1 to random.ceil.toInt) {
        set1 += ((Math.random() * 20).ceil.toInt)
      }
//      val companys = set1.toList.sorted
//      val sb=new StringBuilder
//      companys.foreach(x=>sb.append(x+","))
//      val companyids=sb.toString()
      val companys=set1.mkString(",c_")

      printWriter.println(line + " c_" + companys)
//      printWriter.println(line + " " + companyids.substring(0,companyids.length()-1))
//      sb.clear()
      set1.clear()
    })
    printWriter.close()

  }

}
