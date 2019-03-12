package com.wiseweb.classify

import java.util
import java.util.Properties

import com.wiseweb.util.{ImplicitContext, JedisContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import redis.clients.jedis.{Jedis, Protocol}

import scala.collection.Map
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * Created by yangguihu on 2017/3/8.
  * 将redis中的数据发送到mysql
  */
object SendBusinessDb2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SendBusinessDb").setMaster("local[*]")
      .set("redis.host", "localhost").set("redis.port", "6379")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    sc.setLogLevel("WARN")

    //导入隐式转换函数
    implicit def rdd2rddArrs(sc :SparkContext)= new ImplicitContext(sc)
    //implicit def arr2Map(arr: Array[String])=sc.fromRedisKV(arr).collectAsMap()

    val companys = Array("cp_1","cp_2","cp_3","cp_4","cp_5","cp_6","cp_7","cp_8","cp_9","cp_10")
    //val companys = Array("cp_1")
    import com.redislabs.provider.redis._
    val ssc = new StreamingContext(sc, Seconds(5))
    val redisStream = ssc.createRedisStream(companys, storageLevel = StorageLevel.MEMORY_ONLY)

    //将RDD转换成DataFrame
    import sqlContext.implicits._


    redisStream.foreachRDD(rdd=>{
      rdd.foreachPartition(part3=>{
        val mspTupMap = part3.toMap
        import scala.collection.JavaConversions.setAsJavaSet
        var jSet: java.util.Set[String]=mspTupMap.values.toSet[String]
        //获取返回数据
        //val jedis: Jedis = ConnectionPool.connect(new RedisEndpoint("localhost", 6379, "", 0, Protocol.DEFAULT_TIMEOUT))
        var msgs: java.util.List[String] = JedisContext.msgSet2Map(jSet)

        //关闭jedis
        import scala.collection.JavaConversions.iterableAsScalaIterable
        val scalaMasgs: Iterable[String]=msgs

        for (msg <- scalaMasgs) println(msg)


        //从redis中读取数据到内存中提供组装消息
//        var asMap: Map[String, String] =msgKeys
//        asMap.foreach(println(_))

        //part3.foreach()
      })

      //该rdd中用到的msg的key
//      val msgKeys: Array[String] = rdd.values.distinct().collect()
//
//      //处理数据
//      val rddArr: ArrayBuffer[(String, RDD[String])] = sc.splitRdd2(rdd)
//      //println(rddArr.size)
//      for (tup <- rddArr){
//        //创建Properties存储数据库相关属性
////        val prop = new Properties()
////        prop.put("user", "root")
////        prop.put("password", "BigData123")
////        val msgDF = tup._2.map(json=>Message(json)).toDF()
////        msgDF.write.mode("append").jdbc("jdbc:mysql://127.0.0.1:3306/bigdata", tup._1, prop)
//          tup._2.map(json=>Message(json)).foreach(println(_))
//
//      }
      //获取数据名rdd,用于清除
//      var dtNameRdd = rdd.flatMap(_._2.split("\\|")).distinct()
//      dtNameRdd.foreach(println(_))
//      println()
    })

    ssc.start()
    ssc.awaitTermination()

    sc.stop()
  }
}

/**
  * 发送的消息
  * @param context
  */
case class Message2(context: String)
