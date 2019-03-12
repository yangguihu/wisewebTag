package com.wiseweb.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangguihu on 2017/3/7.
  * spark-redis项目
  *
  * https://github.com/RedisLabs/spark-redis
  */
object SparkJedisDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkJedisDemo").setMaster("local[*]")
      .set("redis.host", "localhost").set("redis.port", "6379")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    import com.redislabs.provider.redis._

    val keysRDD = sc.fromRedisKeyPattern("*", 2)
    //val keysRDD = sc.fromRedisKeys(Array("foo", "bar"), 5)
    keysRDD.foreach(println(_))

    //倒排索引

  }


}
