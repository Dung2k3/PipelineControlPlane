package com.streamflow.node.process.aggregate;

import com.streamflow.expression.ValidSpelExpression;
import com.streamflow.node.process.ProcessNodeConfig;
import com.streamflow.node.process.aggregate.type.AggGroupMode;
import com.streamflow.node.process.aggregate.type.AggregationType;
import com.streamflow.node.process.aggregate.type.WindowType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregateNodeConfig extends ProcessNodeConfig {

    @NotBlank(message = "input la bat buoc")
    private String input;

    @NotNull(message = "groupMode la bat buoc")
    private AggGroupMode groupMode;

    @ValidSpelExpression
    private String keyExtractor;

    @NotEmpty(message = "aggregations la bat buoc, phai co it nhat 1 phan tu")
    @Valid
    private List<AggregationDefinition> aggregations;

    @NotNull(message = "window la bat buoc")
    @Valid
    private AggWindowConfig window;

    @AssertTrue(message = "keyExtractor la bat buoc khi groupMode=GROUP_BY")
    private boolean isKeyExtractorPresentWhenGroupBy() {
        return groupMode != AggGroupMode.GROUP_BY || (keyExtractor != null && !keyExtractor.isBlank());
    }

    @AssertTrue(message = "sourceField la bat buoc voi moi aggregation co type=SUM")
    private boolean isSourceFieldPresentForAllSumAggregations() {
        if (aggregations == null) {
            return true;
        }
        return aggregations.stream()
                .filter(agg -> agg.getType() == AggregationType.SUM)
                .allMatch(agg -> agg.getSourceField() != null && !agg.getSourceField().isBlank());
    }

    @AssertTrue(message = "alias cua tung aggregation phai la duy nhat")
    private boolean isAggregationAliasesUnique() {
        if (aggregations == null) {
            return true;
        }
        long distinctAliasCount =
                aggregations.stream().map(AggregationDefinition::getAlias).distinct().count();
        return distinctAliasCount == aggregations.size();
    }

    @AssertTrue(message = "window.type hien chi ho tro TUMBLING")
    private boolean isTumblingWindow() {
        return window != null && window.getType() == WindowType.TUMBLING;
    }

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

    public AggGroupMode getGroupMode() {
        return groupMode;
    }

    public void setGroupMode(AggGroupMode groupMode) {
        this.groupMode = groupMode;
    }

    public String getKeyExtractor() {
        return keyExtractor;
    }

    public void setKeyExtractor(String keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    public List<AggregationDefinition> getAggregations() {
        return aggregations;
    }

    public void setAggregations(List<AggregationDefinition> aggregations) {
        this.aggregations = aggregations;
    }

    public AggWindowConfig getWindow() {
        return window;
    }

    public void setWindow(AggWindowConfig window) {
        this.window = window;
    }
}
