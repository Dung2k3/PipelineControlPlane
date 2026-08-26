package com.streamflow.node.process.join;

import com.streamflow.expression.ValidSpelExpression;
import com.streamflow.node.process.ProcessNodeConfig;
import com.streamflow.node.process.join.type.JoinType;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;

import java.util.LinkedHashMap;
import java.util.Map;

public class TableJoinNodeConfig extends ProcessNodeConfig {
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
}
