package com.streamflow.node.process.join;

import com.streamflow.expression.ValidSpelExpression;
import com.streamflow.node.process.ProcessNodeConfig;
import com.streamflow.node.process.join.type.JoinType;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.hibernate.validator.constraints.time.DurationMax;
import org.hibernate.validator.constraints.time.DurationMin;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

public class JoinNodeConfig extends ProcessNodeConfig {
    private String leftStream;
    private String rightStream;

    @NotBlank(message = "leftStreamName la bat buoc")
    private String leftStreamName;

    @NotBlank(message = "rightStreamName la bat buoc")
    private String rightStreamName;

    @NotBlank(message = "selectKey la bat buoc")
    @ValidSpelExpression
    private String selectKey;

    private JoinType joinType;

    @NotNull(message = "window la bat buoc")
    @DurationMin(nanos = 1, message = "window phai > 0")
    @DurationMax(minutes = 10, message = "window khong duoc vuot qua 10 phut")
    private Duration window;

    @NotNull(message = "grace la bat buoc")
    @DurationMin(nanos = 0, message = "grace phai >= 0")
    @DurationMax(seconds = 30, message = "grace khong duoc vuot qua 30 giay")
    private Duration grace;

    @AssertTrue(message = "leftStreamName va rightStreamName phai khac nhau")
    private boolean isStreamNameDistinct() {
        return leftStreamName == null || !leftStreamName.equals(rightStreamName);
    }

    @AssertTrue(message = "joinType la bat buoc va hien chi ho tro INNER")
    private boolean isInnerJoin() {
        return joinType == JoinType.INNER;
    }

    @Override
    public Map<String, String> referencedNodeIds() {
        Map<String, String> refs = new LinkedHashMap<>();
        refs.put("leftStream", leftStream);
        refs.put("rightStream", rightStream);
        return refs;
    }

    public String getLeftStream() {
        return leftStream;
    }

    public void setLeftStream(String leftStream) {
        this.leftStream = leftStream;
    }

    public String getRightStream() {
        return rightStream;
    }

    public void setRightStream(String rightStream) {
        this.rightStream = rightStream;
    }

    public String getLeftStreamName() {
        return leftStreamName;
    }

    public void setLeftStreamName(String leftStreamName) {
        this.leftStreamName = leftStreamName;
    }

    public String getRightStreamName() {
        return rightStreamName;
    }

    public void setRightStreamName(String rightStreamName) {
        this.rightStreamName = rightStreamName;
    }

    public String getSelectKey() {
        return selectKey;
    }

    public void setSelectKey(String selectKey) {
        this.selectKey = selectKey;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    public void setJoinType(JoinType joinType) {
        this.joinType = joinType;
    }

    public Duration getWindow() {
        return window;
    }

    public void setWindow(Duration window) {
        this.window = window;
    }

    public Duration getGrace() {
        return grace;
    }

    public void setGrace(Duration grace) {
        this.grace = grace;
    }
}
