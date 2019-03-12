package com.wiseweb.test;

import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Created by yangguihu on 2017/3/13.
 */
public class TestRedis {
    public static void main(String[] args) {
        TestRedis testRedis=new TestRedis();
        testRedis.getListData();
//        testRedis.testListStrem();
    }

    /**
     * 测试list发送数据
     */
    public void testListStrem() {
        Jedis jedis = new Jedis("localhost",6379);
        for (int i = 0; i < 10; i++) {
            String[] ab=new String[10];
            for (int j = 0; j < 10; j++) {
                ab[j]=i+"-"+j;
            }
            jedis.rpush("test",ab);
            try {
                Thread.sleep(5*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        jedis.close();
    }

    public void getListData(){
        Jedis jedis = new Jedis("127.0.0.1", 6379);
        Set<String> keys = jedis.keys("*");
        String[] arr = new String[keys.size()];
        keys.toArray(arr);
//        System.out.println(Arrays.toString(arr));

        Long del = jedis.del(arr);
        System.out.println(del);

//        for (String key : arr) {
//            System.out.print(key+" ==> ");
//            List<String> lrange = jedis.lrange(key, 0, -1);
//            for (String s : lrange) {
//                System.out.print(s+"|");
//            }
//            System.out.println();
//        }
        jedis.close();
    }

}
