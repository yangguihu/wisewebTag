package com.wiseweb.test;

import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Created by yangguihu on 2017/3/14.
 */
public class TestRedis2 {
    public static void main(String[] args) {
        TestRedis2 testredis= new TestRedis2();
        testredis.testListPop();
    }

    public void testListPop() {
        Jedis jedis = new Jedis("localhost",6379);
        while(true){
            List<String> test = jedis.blpop(0,"test");
            //String test = jedis.lpop("test");
            System.out.print(test);
            try {
                Thread.sleep(500);
                System.out.println();
            } catch (InterruptedException e) {
                e.printStackTrace();
                jedis.close();
            }
        }
    }
}
