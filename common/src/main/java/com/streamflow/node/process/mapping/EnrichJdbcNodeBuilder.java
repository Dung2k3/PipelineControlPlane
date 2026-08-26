package com.streamflow.node.process.mapping;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.streamflow.expression.SpelEvaluator;
import com.streamflow.jdbc.JdbcConnectionPool;
import com.streamflow.node.process.ProcessNodeBuilder;
import com.streamflow.topology.TopologyBuildContext;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

public class EnrichJdbcNodeBuilder extends ProcessNodeBuilder<EnrichJdbcNodeConfig> {

    @Override
    protected KStream<String, JsonNode> process(EnrichJdbcNodeConfig node, TopologyBuildContext context) {
        KStream<String, JsonNode> input = context.get(node.getInput());

        List<SpelEvaluator> paramEvaluators = Objects.requireNonNullElse(node.getParams(), List.<String>of())
                .stream()
                .map(SpelEvaluator::new)
                .toList();
        String sql = node.getQuery();
        List<EnrichResultFieldConfig> resultFields = node.getResultFields();

        return input.map((key, value) ->
                KeyValue.pair(key, enrich(key, value, sql, paramEvaluators, resultFields)));
    }

    private JsonNode enrich(
            String key, JsonNode value, String sql, List<SpelEvaluator> paramEvaluators,
            List<EnrichResultFieldConfig> resultFields) {
        ObjectNode enriched = value.deepCopy();
        try (Connection connection = JdbcConnectionPool.dataSource().getConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < paramEvaluators.size(); i++) {
                statement.setString(i + 1, paramEvaluators.get(i).evaluateText(key, value));
            }
            try (ResultSet resultSet = statement.executeQuery()) {
                boolean found = resultSet.next();
                for (EnrichResultFieldConfig field : resultFields) {
                    if (found) {
                        enriched.put(field.getTarget(), resultSet.getString(field.getColumn()));
                    } else {
                        enriched.putNull(field.getTarget());
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Loi JDBC enrich [" + sql + "]: " + e.getMessage(), e);
        }
        return enriched;
    }
}
