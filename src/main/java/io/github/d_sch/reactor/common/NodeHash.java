package io.github.d_sch.reactor.common;

import java.util.Arrays;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.node.ObjectNode;

public interface NodeHash<T> extends Function<ObjectNode, T> {
        static NodeHash<Integer> hashCode(JsonPointer... hashNodes) {
            return objectNode -> Arrays.hashCode(
                    Arrays.stream(hashNodes)
                            .map(
                                    objectNode::at
                            ).toArray()
            );
        }
    }
