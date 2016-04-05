import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.*;

/**
 * Created by vent on 4/5/16.
 * add information to redis by using sorted set.
 */
public class RedisIO {

    public static void zaddCityKey(HashMap<String,HashMap<String,Double>> cityKeyMap)
    {
        JedisPool pool = new JedisPool(new JedisPoolConfig(), "123.59.81.78",6379,20000);
        //Jedis implements Closable. Hence, the jedis instance will be auto-closed after the last statement.
        Jedis jedis = null;
      try {
            jedis = pool.getResource();
            Iterator cityIT = cityKeyMap.keySet().iterator();
            while (cityIT.hasNext())
            {
                String key = (String)cityIT.next();
                System.out.println("-----"+key);
                HashMap<String,Double> keyMap = cityKeyMap.get(key);
                jedis.zadd(key,keyMap);
            }
          System.out.println(          jedis.zrevrange("\"重庆市\"",0,-1));
          System.out.println(          jedis.zrevrange("深圳", 0, -1));
          System.out.println(          jedis.zrevrange("深圳市", 0, -1));

          // Set<String> sose = jedis.zrange("sose", 0, -1);
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
// ... when closing your application:
        pool.destroy();

    }
    public static void printMap(Map mp) {
        Iterator it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map pair = (Map)it.next();
            Iterator pairit = pair.entrySet().iterator();
            while(pairit.hasNext())
            {
                Map.Entry subpair = (Map.Entry)pairit.next();
                System.out.println(subpair.getKey() + " = " + subpair.getValue());
            }
            //System.out.println(pair.getKey() + " = " + pair.getValue());
            it.remove(); // avoids a ConcurrentModificationException
        }
    }
    public static void main(String[] args) {

        HashMap<String,HashMap<String,Double>> cityKeyMap = new HashMap<String,HashMap<String,Double>>();
        HashMap<String,Double> keyMap = new HashMap<String,Double>();
        keyMap.put("小苹果",355.0);
        keyMap.put("tian",15.0);
        keyMap.put("vent",115.0);
        cityKeyMap.put("深圳",keyMap);
        RedisIO.zaddCityKey(cityKeyMap);
    }

    }
