package com.wiseweb.util

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}

import scala.io.Source

/**
  * Created by BFD-595 on 2017/6/7.
  */
object IOTest {
  def main(args: Array[String]): Unit = {


//    val fileWriter = new FileWriter(new File("D:\\JetBrains\\IDEAWorkspace\\wisewebTag\\data\\hive4.txt"));
//    for( a <- 1 to 500000) {
//      val s = a+","+a+"--标题党"+","+a+"--我的问题是怎么使用java将commad的执行结果导出到文件"
//      fileWriter.write(s);
//      fileWriter.write("\n")
//    }
//    fileWriter.close(); // 关闭数据流

    val set1 = Source.fromFile("D:\\JetBrains\\IDEAWorkspace\\wisewebTag\\data\\1.txt").getLines().toSet
    val set2 = Source.fromFile("D:\\JetBrains\\IDEAWorkspace\\wisewebTag\\data\\2.txt").getLines().toSet
    val set3=set1.diff(set2)
    set3.foreach(println(_))

  }

}
