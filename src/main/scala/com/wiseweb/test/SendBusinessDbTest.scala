package com.wiseweb.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yangguihu on 2017/3/8.
  * 将redis中的数据发送到mysql
  */
object SendBusinessDbTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SendBusinessDb").setMaster("local[*]")
      .set("redis.host", "localhost").set("redis.port", "6379")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    //读取redis 倒排公司和消息的数据
    import com.redislabs.provider.redis._
    //公司索引以company_*
    val strRdd = sc.fromRedisKV("cp_*")

//    val jedis = ConnectionPool.connect(RedisEndpoint("localhost",6379,"",0,Protocol.DEFAULT_TIMEOUT))
//    var s: String = jedis.get("dt_8ae82a6c65afc3ce")
//    println(s)
    strRdd.foreach(println(_))
    sc.stop()

  }
}
