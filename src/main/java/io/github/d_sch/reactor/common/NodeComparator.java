package io.github.d_sch.reactor.common;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.TextNode;

public interface NodeComparator extends Comparator<JsonNode> {

    default NodeComparator thenComparing(NodeComparator other) {
        //Objects.requireNonNull(other);
        return (NodeComparator & Serializable) (c1, c2) -> {
            int res = compare(c1, c2);
            return (res != 0) ? res : other.compare(c1, c2);
        };
    }

    static NodeComparator comparing(Function<JsonNode, JsonNode> leftNode, Function<JsonNode, JsonNode> rightNode) {
        return (NodeComparator & Serializable) (left, right) ->
                compareNodes(left != null ? leftNode.apply(left) : null, right != null ? rightNode.apply(right) : null);
    }

    static NodeComparator comparing(String leftPointer, String rightPointer) {
        return comparing(JsonPointer.valueOf(leftPointer), JsonPointer.valueOf(rightPointer));
    }

    static NodeComparator comparing(JsonPointer leftPointer, JsonPointer rightPointer) {
        return (NodeComparator & Serializable) (left, right) ->
                compareNodes(left != null ? left.at(leftPointer) : null, right != null ? right.at(rightPointer) : null);
    }

    static NodeComparator comparingConst(String pointer, Object rightConstant) {
        return comparingConst(JsonPointer.valueOf(pointer), rightConstant);
    }

    static NodeComparator comparingConst(JsonPointer pointer, Object rightConstant) {
        return (NodeComparator & Serializable) (left, right) ->
                compareNodes(
                        getAtLeftOrRight(pointer, left, right),
                        rightConstant
                );
    }

    private static JsonNode getAtLeftOrRight(JsonPointer pointer, JsonNode left, JsonNode right) {
        JsonNode result = null;
        if (left != null) {
            result = left.at(pointer);
        }
        if (result == null && right != null) {
            result = right.at(pointer);
        }
        return result;
    }

    private static int compareNodes(JsonNode left, Object right) {
        if (
                (left == null || left.getNodeType() == JsonNodeType.NULL || 
                left.getNodeType() == JsonNodeType.MISSING) && 
                (right == null)
        ) {
            return 0;
        } else if (
            (
                left == null || left.getNodeType() == JsonNodeType.NULL || 
                left.getNodeType() == JsonNodeType.MISSING) || 
                right == null
        ) {
            return -1;
        }

        switch (left.getNodeType()) {
            case NUMBER -> {
                if (left instanceof NumericNode leftNumeric && right instanceof Number rightNumeric) {
                    return compareNumericNodeWithNumber(leftNumeric, rightNumeric);
                } else {
                    throw new IllegalArgumentException("Expected both nodes of numeric type! " + left.toString() + "," + right.toString());
                }
            }
            case STRING -> {
                if (left instanceof TextNode leftText && right instanceof String rightText) {
                    return leftText.textValue().compareTo(rightText);
                } else {
                    throw new IllegalArgumentException("Expected both nodes of text type!" + left.toString() + "," + right.toString());
                }
            }
            case BOOLEAN -> {
                if (left instanceof BooleanNode leftBoolean && right instanceof Boolean rightBoolean) {
                    return Boolean.compare(leftBoolean.booleanValue(), rightBoolean);
                } else {
                    throw new IllegalArgumentException("Expected both nodes of boolean type!" + left.toString() + "," + right.toString());
                }
            }
            default -> throw new IllegalArgumentException("Expected number, text or boolean nodes!");
        }
    }

    private static int compareNodes(JsonNode left, JsonNode right) {
        if ((left == null || left.getNodeType() == JsonNodeType.NULL || left.getNodeType() == JsonNodeType.MISSING) &&
                (right == null || right.getNodeType() == JsonNodeType.NULL || right.getNodeType() == JsonNodeType.MISSING)) {
            return 0;
        } else if ((left == null || left.getNodeType() == JsonNodeType.NULL || left.getNodeType() == JsonNodeType.MISSING) ||
                (right == null || right.getNodeType() == JsonNodeType.NULL || right.getNodeType() == JsonNodeType.MISSING)) {
            return -1;
        }
        switch (left.getNodeType()) {
            case NUMBER -> {
                if (left instanceof NumericNode leftNumeric && right instanceof NumericNode rightNumeric) {
                    return compareNumericNodes(leftNumeric, rightNumeric);
                } else {
                    throw new IllegalArgumentException("Expected both nodes of numeric type! " + left.toString() + "," + right.toString());
                }
            }
            case STRING -> {
                if (left instanceof TextNode leftText && right instanceof TextNode rightText) {
                    return leftText.textValue().compareTo(rightText.textValue());
                } else {
                    throw new IllegalArgumentException("Expected both nodes of text type!" + left.toString() + "," + right.toString());
                }
            }
            case BOOLEAN -> {
                if (left instanceof BooleanNode leftBoolean && right instanceof BooleanNode rightBoolean) {
                    return Boolean.compare(leftBoolean.booleanValue(), rightBoolean.booleanValue());
                } else {
                    throw new IllegalArgumentException("Expected both nodes of boolean type!" + left.toString() + "," + right.toString());
                }
            }
            default -> throw new IllegalArgumentException("Expected number, text or boolean nodes!");
        }
    }

    private static int compareNumericNodeWithNumber(NumericNode left, Number right) {
        if (left instanceof IntNode leftInt) {
            return Integer.compare(leftInt.intValue(), right.intValue());
        } else if (left instanceof LongNode leftLong) {
            return Long.compare(leftLong.longValue(), right.longValue());
        } else if (left instanceof DoubleNode leftDouble) {
            return Double.compare(leftDouble.doubleValue(), right.doubleValue());
        } else if (left instanceof DecimalNode leftDecimal) {
            return leftDecimal.decimalValue().compareTo((BigDecimal) right);
        } else if (left instanceof BigIntegerNode leftBigInteger) {
            return leftBigInteger.bigIntegerValue().compareTo((BigInteger) right);
        } else {
            throw new IllegalArgumentException("Expected int, long, double, decimal or big integer nodes!");
        }
    }

    private static int compareNumericNodes(NumericNode left, NumericNode right) {
        if (!left.getClass().isInstance(right)) {
            throw new IllegalArgumentException("Expected both nodes of same number type!"  + left.getClass().getName() + "," + right.getClass().getName());
        }
        if (left instanceof IntNode leftInt) {
            return Integer.compare(leftInt.intValue(), right.intValue());
        } else if (left instanceof LongNode leftLong) {
            return Long.compare(leftLong.longValue(), right.longValue());
        } else if (left instanceof DoubleNode leftDouble) {
            return Double.compare(leftDouble.doubleValue(), right.doubleValue());
        } else if (left instanceof DecimalNode leftDecimal) {
            return leftDecimal.decimalValue().compareTo(right.decimalValue());
        } else if (left instanceof BigIntegerNode leftBigInteger) {
            return leftBigInteger.bigIntegerValue().compareTo(right.bigIntegerValue());
        } else {
            throw new IllegalArgumentException("Expected int, long, double, decimal or big integer nodes!");
        }
    }
}