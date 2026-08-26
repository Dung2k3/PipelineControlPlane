package com.streamflow.cache;

import com.streamflow.controlplane.config.AppConfig;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;


public final class RedisConnectionPool {

    private static final JedisPool POOL = build();

    private RedisConnectionPool() {
    }

    public static JedisPool pool() {
        return POOL;
    }

    private static JedisPool build() {
        String host = AppConfig.get("REDIS_HOST");
        int port = Integer.parseInt(AppConfig.get("REDIS_PORT"));
        return new JedisPool(new JedisPoolConfig(), host, port);
    }
}
