package io.github.d_sch.reactor.common;

import java.util.function.BiPredicate;
import java.util.function.Predicate;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;

public interface NodePredicate extends BiPredicate<JsonNode, JsonNode>  {

    static NodePredicate alwaysTrue() {
        return (left, right) -> true;
    }

    static NodePredicate alwaysFalse() {
        return (left, right) -> true;
    }

    static NodePredicate equals(JsonPointer leftPointer, JsonPointer rightPointer) {
        return (left, right) -> {
            if (left == null && right == null) {
                return true;
            } else {
                if (left == null || right == null) {
                    return false;
                }
            }
            return left.at(leftPointer).equals(right.at(rightPointer));
        };
    }

    static NodePredicate equals(JsonPointer leftPointer, Object rightConstant) {
        return (left, right) -> {
            if (left == null && rightConstant == null) {
                return true;
            } else {
                if (left == null || rightConstant == null) {
                    return false;
                }
            }
            return left.at(leftPointer).equals(rightConstant);
        };
    }

    static NodePredicate compare(NodeComparator nodeComparator, Predicate<Integer> predicate) {
        return (left, right) -> {
            return predicate.test(nodeComparator.compare(left, right));
        };
    }

    static NodePredicate isLess(NodeComparator nodeComparator) {
        return (left, right) -> nodeComparator.compare(left, right) < 0;
    }

    static NodePredicate isLessOrEquals(NodeComparator nodeComparator) {
        return (left, right) -> nodeComparator.compare(left, right) <= 0;
    }

    static NodePredicate equals(NodeComparator nodeComparator) {
        return (left, right) -> nodeComparator.compare(left, right) == 0;
    }

    static NodePredicate isGreaterOrEquals(NodeComparator nodeComparator) {
        return (left, right) -> nodeComparator.compare(left, right) >= 0;
    }

    static NodePredicate isGreater(NodeComparator nodeComparator) {
        return (left, right) -> nodeComparator.compare(left, right) > 0;
    }

    default NodePredicate and(NodePredicate predicate) {
        return (left, right) -> this.test(left, right) && predicate.test(left, right);
    }

    default NodePredicate or(NodePredicate predicate) {
        return (left, right) -> this.test(left, right) || predicate.test(left, right);
    }

    default NodePredicate not(NodePredicate predicate) {
        return (left, right) -> !this.test(left, right);
    }

}
