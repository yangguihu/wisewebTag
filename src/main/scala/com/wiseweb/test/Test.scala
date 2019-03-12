package com.wiseweb.test

import java.io.{File, PrintWriter, StringReader}

import com.alibaba.fastjson.JSON
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.wltea.analyzer.lucene.IKAnalyzer
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.io.Source

object Test {
  def main(args: Array[String]): Unit = {

  }

  //测试list结构流处理过程
  def testListStrem(): Unit ={
    var jedis: Jedis = new Jedis("localhost",6379)
    for(i <- 1 to 9){
      val ab = ArrayBuffer[String]()
      for (j<- 1 to 9){
        ab += i+"-"+j
      }
      //jedis.lpush("test",ab)

    }

  }

  //set集合过滤
  def setFiter(): Unit ={
    //parseFiletoKeywords()
    val key1="data/key1.txt"
    val key2="data/key2.txt"
    val key1List=new ListBuffer[String]()
    Source.fromFile(key1).getLines().foreach(str=>key1List.+=(str.trim))
    val key2List=new ListBuffer[String]()
    Source.fromFile(key2).getLines().foreach(str=>key2List.+=(str.trim))
    val filter = key1List.filter(key2List.contains(_))
    print(filter)

  }

  //解析关键词
  def parseFiletoKeywords(): Unit ={
    /*解析新闻  分词 生成关键词 写到文件*/
    val analyzer = new IKAnalyzer(true);
    val keywordsSet = new mutable.HashSet[String]()
    val lines = Source.fromFile("data/1008num.txt").getLines()
    lines.foreach(line=>{
      val jstr = JSON.parseObject(line)
      val content = jstr.getString("content")
      val reader = new StringReader(content);
      val tokenStream=analyzer.tokenStream("", reader);
      val term =tokenStream.getAttribute(classOf[CharTermAttribute]);
      tokenStream.reset()
      while(tokenStream.incrementToken()){
        keywordsSet.+=(term.toString())
      }
      tokenStream.end()
      tokenStream.close()
    })
    //写到文件
    //负面舆情关键词写到文件
    val printWriter: PrintWriter = new PrintWriter(new File("F:\\IdeaTestData\\testData\\key1.txt"))
    keywordsSet.foreach(word=>if (!word.isEmpty) {printWriter.println(word)})
    printWriter.close()
  }

}
