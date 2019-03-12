package com.wiseweb.test

import java.io.StringReader

import com.alibaba.fastjson.JSON
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.lucene.IKAnalyzer

import scala.collection.{Map, mutable}

object ikTest {
  def main(args: Array[String]): Unit = {
//      val key1="F:\\IdeaTestData\\testData\\key1.txt"
//      val key2="F:\\IdeaTestData\\testData\\key2.txt"
//      val key1List=new ListBuffer[String]()
//      Source.fromFile(key1).getLines().foreach(str=>key1List.+=(str.trim))
//      val key2List=new ListBuffer[String]()
//      Source.fromFile(key2).getLines().foreach(str=>key2List.+=(str.trim))
//      val filter = key1List.filter(key2List.contains(_))
//      print(filter)
    parseFiletoKeywords();
  }
  def parseFiletoKeywords(): Unit ={
    val conf = new SparkConf().setAppName("ikTestSpark").setMaster("local[*]")
//    conf.registerKryoClasses(Array(classOf[org.wltea.analyzer.lucene.IKAnalyzer]))
    val sc = new SparkContext(conf)
    val lines = sc.textFile("D:\\iktest\\new 1.txt")
    val linesRegular = sc.textFile("D:\\iktest\\regular2.txt")
    val reg=linesRegular.map(ts=>{
      val strings: Array[String] = ts.split(" ")
      val toSet: Set[String] = strings(1).split(",").toSet
      (strings(0),toSet)
    }).collectAsMap()
    val broadcast: Broadcast[Map[String, Set[String]]] = sc.broadcast(reg)
//    mp.foreach(println(_))
    val mapSets = lines.map(line=>{
    /*解析新闻  分词 生成关键词 写到文件*/
      val analyzer = new IKAnalyzer(true);
      val keywordsSet = new mutable.HashSet[String]()
      val jstr = JSON.parseObject(line)
      val content = jstr.getString("content")
      val reader = new StringReader(content);
      val tokenStream=analyzer.tokenStream("", reader);
      val term =tokenStream.getAttribute(classOf[CharTermAttribute]);
      tokenStream.reset()
      while(tokenStream.incrementToken()){
        keywordsSet.+=(term.toString())
      }
      tokenStream.end();
      tokenStream.close();
       val rules: Map[String, Set[String]] = broadcast.value
     val finaMap = rules.filter(ts=>{
        keywordsSet.contains(ts._1)
      })
//  println(finaMap)
      val finaSet = new mutable.HashSet[String]()
      finaMap.foreach(f=>{
        finaSet++=f._2
      })
//  println(finaSet)
      keywordsSet.clear()
      (line,finaSet)
    })

   val values = mapSets.flatMapValues(it=>{it.mkString(",").split(",")}).map(p=>{
      val s2: String = p._2
      val strs=p._1
      (s2,strs)
    })
    values.foreach(println(_))
    //写到文件
    //负面舆情关键词写到文件
//    val printWriter: PrintWriter = new PrintWriter(new File("D:\\keywords.txt"))
//    keywordsSet.foreach(word=>if (!word.isEmpty) {printWriter.println(word)})
//    printWriter.close()
  }

}
