package com.streamflow.node.process.cache;

import com.fasterxml.jackson.databind.JsonNode;
import com.streamflow.cache.RedisConnectionPool;
import com.streamflow.expression.SpelEvaluator;
import com.streamflow.topology.NodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.streams.kstream.KStream;
import redis.clients.jedis.Jedis;

public class CacheSinkNodeBuilder implements NodeBuilder<CacheSinkNodeConfig> {

    @Override
    public void build(CacheSinkNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> input = context.get(node.getInput());
        SpelEvaluator keyEvaluator = new SpelEvaluator(node.getKeyExpr());

        input.foreach((key, value) -> {
            String redisKey = keyEvaluator.evaluateText(key, value);
            try (Jedis jedis = RedisConnectionPool.pool().getResource()) {
                if (node.getMode() == CacheMode.LIST_APPEND) {
                    jedis.rpush(redisKey, value.toString());
                } else {
                    jedis.set(redisKey, value.toString());
                }
            }
        });
    }
}
