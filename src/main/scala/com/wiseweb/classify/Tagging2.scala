package com.wiseweb.classify

import java.io.StringReader

import com.alibaba.fastjson.JSON
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.lucene.IKAnalyzer

import scala.collection.mutable

/**
  * Created by yangguihu on 2017/3/6.
  *
  */
object Tagging2 {


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Tagging").setMaster("local[*]")
      .set("redis.host", "localhost").set("redis.port", "6379")
      .registerKryoClasses(Array(classOf[SparkConf]))

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //获取模拟规则数据
    var ruleRdd= sc.textFile("data/rule.txt")
    val ruleSet= ruleRdd.map(_.split(" ")(0)).collect().toSet
    //规则map
    val ruleMap=ruleRdd.map(line=>{
      val split = line.split(" ")
      //收集成为map
      val compannySet=split(1).split(",").toSet
      (split(0),compannySet)
    }).collectAsMap()

//    println(ruleMap)
//    println(ruleSet)

    //广播规则相关set和集合
    val bcRuleSet = sc.broadcast(ruleSet)
    val bcRuleMap = sc.broadcast(ruleMap)

    val sourceRdd=sc.textFile("data/json1.txt")

    val msgkvRdd=sourceRdd.mapPartitions(it=>{
      it.map(line=>{
        val jstr = JSON.parseObject(line)
        val urlhash=jstr.getString("urlhash")
        ("dt_"+urlhash,line)
      })
    })
    //将数据保存到redis中
    import com.redislabs.provider.redis._
    sc.toRedisKV(msgkvRdd)

    //处理数据映射
    val mapRdd=sourceRdd.mapPartitions(it=>{
      val analyzer = new IKAnalyzer(true);
      //处理每一条
      it.map(line=>{
        val jstr = JSON.parseObject(line)
        val urlhash=jstr.getString("urlhash")
        val content = jstr.getString("content")

        val reader = new StringReader(content)

        val tokenStream=analyzer.tokenStream("", reader)
        val term =tokenStream.getAttribute(classOf[CharTermAttribute])
        tokenStream.reset()
        //存储分词后集合
        val keywordsSet = new mutable.HashSet[String]()
        while(tokenStream.incrementToken()){
          keywordsSet += term.toString()
        }
        tokenStream.end()
        tokenStream.close()

        //交集集合
        val rules=bcRuleSet.value
        val selSet= keywordsSet.filter(rules)

        val ruleMap1=bcRuleMap.value
  //      accCompanys(selSet,ruleMap)

        val finaSet = new mutable.HashSet[String]()
        selSet.foreach(key => {
          var get1 = ruleMap1.getOrElse(key, Nil)
          finaSet ++= get1
        })
        //值设置成索引
        //val companys=finaSet.mkString("|")
        //清除分词集合，下次使用
        //val companys = finaSet.fold("")((a,b)=>a+"|"+b)
        (urlhash,finaSet)
        //finaSet.clear()
        //返回元组
        //(urlhash,companys)
      })
    })

    //倒排rdd
    val invertSortRdd=mapRdd.flatMapValues(_.mkString(",").split(",")).map(tup=>("cp_"+tup._2,"dt_"+tup._1)).reduceByKey(_+"|"+_)

    var collect: Array[String] = invertSortRdd.flatMap(_._2.split("\\|")).distinct().collect()
    collect.foreach(println(_))
    //以list的方式存到redis
//    invertSortRdd.foreach(it=>{
//      var split: Array[String] = it._2.split("|")
//      sc.toRedisLIST(listRDD,it._1)
//    })
    //底层是以rpush的方式推送，stream获取的时候采用blpop方式拉取，保证先进先出
    sc.toRedisLIST2(invertSortRdd,"\\|")

    //以list的方式存到redis
//    invertSortRdd.foreach(it=>{
//      val sc1 = new SparkContext(conf)
//      //var split: Array[String] = it._2.split("|")
//      val listRDD = sc1.parallelize(it._2.split("|"))
//      sc1.toRedisLIST(listRDD,it._1)
//    })
//    var count = invertSortRdd.count()
//
//    var split: Array[RDD[(String, String)]] = invertSortRdd.randomSplit(Array.fill(count.toInt)(1.0/count))
//    split.foreach(r=>println(r.collect().mkString(" ")))


    //保存倒排rdd到redis
//    sc.toRedisKV(invertSortRdd)

    sc.stop()

  }
}
