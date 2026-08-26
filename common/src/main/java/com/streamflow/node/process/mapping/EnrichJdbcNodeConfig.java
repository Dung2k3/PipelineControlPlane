package com.streamflow.node.process.mapping;

import com.streamflow.expression.ValidSpelExpression;
import com.streamflow.node.process.ProcessNodeConfig;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Enrich 1 KStream bang 1 query JDBC dong bo moi record - dung khi du lieu tham chieu nam o 1
 * database ngoai (khong phai Kafka topic). Giu nguyen moi field cu cua input, chi cong them
 * resultFields tu ResultSet - khac MAPPING (dung lai value hoan toan moi tu fields).
 */
public class EnrichJdbcNodeConfig extends ProcessNodeConfig {

    @NotBlank(message = "input la bat buoc")
    private String input;

    @NotBlank(message = "query la bat buoc")
    private String query;

    private List<@ValidSpelExpression String> params;

    @NotEmpty(message = "resultFields la bat buoc, phai co it nhat 1 field")
    @Valid
    private List<EnrichResultFieldConfig> resultFields;

    @Override
    public Map<String, String> referencedNodeIds() {
        Map<String, String> refs = new LinkedHashMap<>();
        refs.put("input", input);
        return refs;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public List<String> getParams() {
        return params;
    }

    public void setParams(List<String> params) {
        this.params = params;
    }

    public List<EnrichResultFieldConfig> getResultFields() {
        return resultFields;
    }

    public void setResultFields(List<EnrichResultFieldConfig> resultFields) {
        this.resultFields = resultFields;
    }
}
