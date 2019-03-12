package com.wiseweb.classify

import java.io.StringReader

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wiseweb.util.ImplicitContext
import kafka.serializer.StringDecoder
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.lucene.IKAnalyzer

import scala.collection.immutable.Iterable
import scala.collection.{GenTraversableOnce, Map, immutable, mutable}
import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Created by yangguihu on 2017/3/6.
  * 本示例以论坛回帖topic为源，具体根据业务进行改造
  *
  */
object Tagging {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Tagging").setMaster("local[*]")
      .set("redis.host", "localhost").set("redis.port", "6379")
      //设置最大拉取长度
      .set("spark.streaming.kafka.maxRatePerPartition","1")
      //.registerKryoClasses(Array(classOf[SparkConf]))

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    //加载规则库
    var ruleRdd= sc.textFile("data/rule.txt")
    val ruleSet= ruleRdd.map(_.split(" ")(0)).collect().toSet
    //println(ruleSet)
    //规则map
    val ruleMap=ruleRdd.map(line=>{
      val split = line.split(" ")
      //收集成为map
      val compannySet=split(1).split(",").toSet
      (split(0),compannySet)
    }).collectAsMap()

    //广播规则相关set和集合
    val bcRuleSet = sc.broadcast(ruleSet)
    val bcRuleMap = sc.broadcast(ruleMap)

    //kafka直连流获取数据
    val brokers = "node1:9092,node2:9092,node3:9092,hadoop1:9092,hadoop2:9092"
    val topics = "wiseweb_crawler_replies"
    val group = "replies_tagging"

    val topicsSet = topics.split(",").toSet
    val ssc = new StreamingContext(sc, Seconds(30))

    val kafkaParams = immutable.Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> group,
      "fetch.message.max.bytes" -> "10485760"
      ,"auto.offset.reset" -> "smallest"
    )
    val km = new KafkaManager(kafkaParams)
    val messages = km.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //处理消息
    messages.foreachRDD(sourceRdd=>{
        //获取消息urlahash和消息映射集合
        val msgkvRdd=sourceRdd.mapPartitions(it=>{
          it.map(line=>{
            //println(line._2)
            //line._1 为偏移量
            val jstr = JSON.parseObject(line._2)
            val urlhash=jstr.getString("urlhash")
            //println(urlhash)
            (urlhash,jstr,line._2)
          })
        })
        //消息map
//        val msgMap: Map[String, String] = msgkvRdd.collectAsMap()
        val msgMap: Map[String, String] = msgkvRdd.map(msg=>(msg._1,msg._3)).collectAsMap()
        val jsonObjectRdd: RDD[JSONObject] = msgkvRdd.mapPartitions(_.map(_._2))

        //处理数据映射
        val mapRdd=jsonObjectRdd.mapPartitions(it=>{
          val analyzer = new IKAnalyzer(true);
          //存储分词后集合
          val keywordsSet = new mutable.HashSet[String]()
          //交集集合
          val rules=bcRuleSet.value
          val ruleMap1=bcRuleMap.value

          it.map(job=>{ //处理每一个JSONObject
            val urlhash=job.getString("urlhash")
            //回帖replycontent
            val content = job.getString("replycontent")

            val reader = new StringReader(content)

            val tokenStream=analyzer.tokenStream("", reader)
            val term =tokenStream.getAttribute(classOf[CharTermAttribute])
            tokenStream.reset()

            keywordsSet.clear() //清除上条集合
            while(tokenStream.incrementToken()){
              keywordsSet += term.toString()
            }
            tokenStream.end()
            tokenStream.close()

            //过滤出选中的词
            val selSet= keywordsSet.filter(rules)
            val finaSet = new mutable.HashSet[String]()
            selSet.foreach(key => {
              var get1 = ruleMap1.getOrElse(key, Nil)
              finaSet ++= get1
            })
            //值设置成索引
            (urlhash,finaSet)
          })
        })
//        mapRdd.foreach(println(_))
        //倒排rdd
        val invertSortRdd=mapRdd.flatMapValues(_.mkString(",").split(",")).map(tup=>("cp_"+tup._2,tup._1)).reduceByKey(_+"|"+_)
//        var collect: Array[String] = invertSortRdd.flatMap(_._2.split("\\|")).distinct().collect()
//        collect.foreach(println(_))

        implicit def rdd2rddArrs(sc :SparkContext)= new ImplicitContext(sc)
        //底层是以rpush的方式推送，stream获取的时候采用blpop方式拉取，保证先进先出
        sc.toRedisLIST3(invertSortRdd,msgMap,"\\|")

        // 再更新offsets
        km.updateZKOffsets(sourceRdd)
    })
    // 开始处理
    ssc.start()
    ssc.awaitTermination()

  }
}
