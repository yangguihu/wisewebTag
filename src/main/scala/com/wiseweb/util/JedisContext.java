package com.wiseweb.util;

import com.redislabs.provider.redis.ConnectionPool;
import com.redislabs.provider.redis.RedisEndpoint;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yangguihu on 2017/3/15.
 */
public class JedisContext {
    static Jedis jedis=new Jedis("127.0.0.1", 6379, Integer.MAX_VALUE);
    public static List<String> msgSet2Map(Set<String> msgKeySet){
        String[] msgkeys = msgKeySet.toArray(new String[msgKeySet.size()]);
        if(!jedis.isConnected()){
            jedis = new Jedis("127.0.0.1", 6379, Integer.MAX_VALUE);
        }
        List<String> mget = jedis.mget(msgkeys);
        return mget;
    }
    public static  void closeJedisConn(){
        jedis.close();
    }

}
