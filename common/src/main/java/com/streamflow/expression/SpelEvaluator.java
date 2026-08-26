package com.streamflow.expression;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

public class SpelEvaluator {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final SpelExpressionParser PARSER = new SpelExpressionParser();
    private static final TemplateParserContext TEMPLATE_CONTEXT = new TemplateParserContext("@{", "}");

    private final Expression expression;

    public SpelEvaluator(String spelExpression) {
        try {
            this.expression = spelExpression.contains("@{")
                    ? PARSER.parseExpression(spelExpression, TEMPLATE_CONTEXT)
                    : PARSER.parseExpression(spelExpression);
        } catch (Exception e) {
            throw new IllegalArgumentException("bieu thuc SpEL khong hop le: " + spelExpression, e);
        }
    }

    public String evaluateText(String key, JsonNode value) {
        try {
            Object result = rawEvaluate(key, value);
            if (result == null) {
                throw new SpelExpressionException(
                        "bieu thuc [" + expression.getExpressionString()
                                + "] tra ve null cho key=" + key + ", value=" + value, null);
            }
            if (result instanceof JsonNode node) {
                if (node.isMissingNode() || node.isNull()) {
                    throw new SpelExpressionException(
                            "bieu thuc [" + expression.getExpressionString()
                                    + "] khong tim thay field (missing/null) cho key=" + key
                                    + ", value=" + value, null);
                }
                return node.asText();
            }
            return String.valueOf(result);
        } catch (SpelExpressionException e) {
            throw e;
        } catch (Exception e) {
            throw new SpelExpressionException(
                    "Loi evaluate bieu thuc [" + expression.getExpressionString()
                            + "] tren key=" + key + ", value=" + value, e);
        }
    }

    public boolean evaluateBoolean(String key, JsonNode value) {
        try {
            Boolean result = expression.getValue(new StandardEvaluationContext(new SpelRoot(key, value)), Boolean.class);
            if (result == null) {
                throw new SpelExpressionException(
                        "bieu thuc [" + expression.getExpressionString()
                                + "] tra ve null cho key=" + key + ", value=" + value, null);
            }
            return result;
        } catch (SpelExpressionException e) {
            throw e;
        } catch (Exception e) {
            throw new SpelExpressionException(
                    "Loi evaluate bieu thuc [" + expression.getExpressionString()
                            + "] tren key=" + key + ", value=" + value, e);
        }
    }

    public JsonNode evaluateNode(String key, JsonNode value) {
        try {
            Object result = rawEvaluate(key, value);
            if (result == null) {
                return MAPPER.getNodeFactory().nullNode();
            }
            if (result instanceof JsonNode node) {
                if (node.isMissingNode()) {
                    throw new SpelExpressionException(
                            "bieu thuc [" + expression.getExpressionString()
                                    + "] khong tim thay field (missing) cho key=" + key
                                    + ", value=" + value, null);
                }
                return node;
            }
            return MAPPER.valueToTree(result);
        } catch (SpelExpressionException e) {
            throw e;
        } catch (Exception e) {
            throw new SpelExpressionException(
                    "Loi evaluate bieu thuc [" + expression.getExpressionString()
                            + "] tren key=" + key + ", value=" + value, e);
        }
    }

    private Object rawEvaluate(String key, JsonNode value) {
        return expression.getValue(new StandardEvaluationContext(new SpelRoot(key, value)));
    }
}
