package com.wiseweb.test

import redis.clients.jedis.Jedis

/**
  * Created by yangguihu on 2017/3/14.
  * 模拟测试数据到list结构的redis
  */
object WriteList2redis {
  def main(args: Array[String]) {
    //企业
    val companys = Array("cp_1","cp_2","cp_3","cp_4","cp_5","cp_6","cp_7","cp_8","cp_9","cp_10")
    //数据
    val datas=Array("dt_6d7241c158ca1ee5","dt_ec49e3e1004aaafa","dt_7ea1b5d72fbc410e","dt_05cfe48283dab4d8",
      "dt_685dcf1e213744bf","dt_e28b4e679ec1d064","dt_3f26e737ab9cd30e","dt_5371081463c903f5",
      "dt_06a664312c6c0f01","dt_ed2b82fc1506865e")

    sendMultiCp(companys, datas)
//    sendSingleCp(companys, datas)

  }

  /**
    * 发送到单个公司
    * @param companys
    * @param datas
    */
  def sendSingleCp(companys: Array[String], datas: Array[String]): Unit ={
    var jedis = new Jedis("127.0.0.1", 6379)
    while(true){
      val random = Math.random() * 10
      for (i <- 1 to random.ceil.toInt) {
        var index = (Math.random() * 10).ceil.toInt
        if (index == 10) index = 0
        jedis.rpush("cp_1", datas(index))
        print(datas(index) + ",")
      }
      println()
      Thread.sleep(3*1000)
    }
    jedis.close()
  }



  /**
    * 发送到多个公司
    * @param companys
    * @param datas
    */
  def sendMultiCp(companys: Array[String], datas: Array[String]): Unit = {
    while (true) {
      var jedis = new Jedis("127.0.0.1", 6379)
      for (company <- companys) {
        print(company + "==> ")
        val random = Math.random() * 10
        for (i <- 1 to random.ceil.toInt) {
          var index = (Math.random() * 10).ceil.toInt
          if (index == 10) index = 0
          jedis.rpush(company, datas(index))
          print(datas(index) + ",")
        }
        println()
      }
      jedis.close()
      Thread.sleep(2 * 1000)
    }
  }
}
