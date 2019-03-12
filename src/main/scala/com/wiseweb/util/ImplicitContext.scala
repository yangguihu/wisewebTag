package com.wiseweb.util

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import com.redislabs.provider.redis._

/**
  * Created by yangguihu on 2017/3/14.
  * 根据业务对sparkContext进行隐式转换达到方法增强
  */
class ImplicitContext(@transient val sc: SparkContext) extends Serializable{
  /**
    * @param vs  RDD of (key,values)
    * @param delimited    value的分隔符
    * @param ttl
    * @param redisConfig
    */
  def toRedisLIST3(vs: RDD[(String,String)],msgMap: Map[String, String],delimited: String, ttl: Int = 0)
                  (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
    vs.foreachPartition(partition => {
      partition.foreach(line=>{
        val toIterator = line._2.split(delimited).toIterator
        setList3(line._1, toIterator, msgMap, ttl, redisConfig)
      })
    })
  }

  /**
    * 根据传入的urlhash,从msgMap中获取值，替换成数据进行发送
    * @param listName
    * @param arr values which should be saved in the target host
    *            save all the values to listName(list type) to the target host
    * @param ttl time to live
    */
  def setList3(listName: String, arr: Iterator[String],msgMap: Map[String, String],ttl: Int, redisConfig: RedisConfig) {
    val conn = redisConfig.connectionForKey(listName)
    val pipeline = conn.pipelined
    //替换发送真实数据
    arr.foreach(it=>{
      val msg: String = msgMap.getOrElse(it,"")
      pipeline.rpush(listName, msg)
    })
    if (ttl > 0) pipeline.expire(listName, ttl)
    pipeline.sync
    conn.close
  }

  /**
    * 根据对偶rdd的可以来过滤返回多个rdd
    * @param vs
    * @return
    */
  def splitRdd(vs: RDD[(String,String)]): ArrayBuffer[RDD[(String, String)]] ={
    val splitCount=vs.mapPartitions(part=>{part.map(_._1)}).distinct().collect()
    val rddArrs = ArrayBuffer[RDD[(String, String)]]()
    for(key <- splitCount){
      val filterRdd: RDD[(String, String)] = vs.filter(_._1.equals(key))
      rddArrs +=filterRdd
    }
    rddArrs
  }

  /**
    * 根据对偶rdd的可以来过滤返回多个rdd2，key提取出一个
    * @param vs
    * @return
    */
  def splitRdd2(vs: RDD[(String,String)]): ArrayBuffer[(String,RDD[String])] ={
    val splitCount=vs.mapPartitions(part=>{part.map(_._1)}).distinct().collect()
    val rddArrs = ArrayBuffer[(String,RDD[String])]()
    for(key <- splitCount){
      val filterRdd: RDD[(String, String)] = vs.filter(_._1.equals(key))
      val valuesRdd: RDD[String] = filterRdd.mapPartitions(part2=>part2.map(_._2))
      val tup=(key,valuesRdd)
      rddArrs += tup
    }
    rddArrs
  }

  /**
    * 根据输入的keys 从redis中获取对应数据，并作为一个map返回
    * @param msgKeys  redis  keys
    * @return
    */
  def keys2Map4Redis(msgKeys: Array[String]): Map[String, String] ={
    sc.fromRedisKV(msgKeys).collectAsMap()
  }
}
