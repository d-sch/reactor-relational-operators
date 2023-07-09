package io.github.d_sch.webfluxcommon;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import io.github.d_sch.reactor.common.NodeComparator;
import io.github.d_sch.reactor.common.NodeHash;
import io.github.d_sch.reactor.common.NodePredicate;
import io.github.d_sch.reactor.operators.FluxHashMatchJoin;
import io.github.d_sch.reactor.operators.FluxMergeJoin;
import io.github.d_sch.reactor.operators.HashMatchJoinType;
import io.github.d_sch.reactor.operators.MergeJoinType;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

class TestClass {

    @Test
    void testMergeJoin() throws JsonProcessingException {
        var om = new ObjectMapper();

        final var comparator = NodeComparator.comparing(
                JsonPointer.valueOf("/a/customerId"),
                JsonPointer.valueOf("/b/customerId")
        );

        final var sortComparator = NodeComparator.comparing(
                JsonPointer.valueOf("/a/customerId"),
                JsonPointer.valueOf("/a/customerId")
        );

        final var predicate =
                NodePredicate.alwaysTrue();
//                NodePredicate.compare(
//                        NodeComparator.comparingConst("/a/customerId", "2"), c -> c >= 0
//                );
//                .and(
//                        NodePredicate.compare(
//                                NodeComparator.comparingConst("/a/customerId", "6"), c -> c != 0
//                        )
//                );

//                        (left != null && NodeComparator.compareNodes(left.get("numberOfInquiries"), 2) >= 0) ||
//                                (right != null && NodeComparator.compareNodes(right.get("numberOfInquiries"), 6) != 0);

        var left = Mono.just(
            Arrays.asList(
                    om.readTree("{\"a\": {\"customerId\": \"1\", \"numberOfInquiries\": 1}}"),
                    om.readTree("{\"a\": {\"customerId\": \"5\", \"numberOfInquiries\": 2}}"),
                    om.readTree("{\"a\": {\"customerId\": \"2\", \"numberOfInquiries\": 4}}"),
                    om.readTree("{\"a\": {\"customerId\": \"6\", \"numberOfInquiries\": 5}}")
            )
        ).flatMapMany(list -> Flux.fromIterable(list).sort(sortComparator).cast(ObjectNode.class));

        System.out.println("LEFT: " + om.valueToTree(left.collectList().block()).toPrettyString());

        var right = Mono.just(
                Arrays.asList(
                        om.readTree("{\"b\": {\"customerId\": \"1\", \"numberOfInquiries\": 3}}"),
                        om.readTree("{\"b\": {\"customerId\": \"3\", \"numberOfInquiries\": 6}}"),
                        om.readTree("{\"b\": {\"customerId\": \"5\", \"numberOfInquiries\": 7}}")
                )
        ).flatMapMany(list -> Flux.fromIterable(list).cast(ObjectNode.class));

        System.out.println("RIGHT: " + om.valueToTree(right.collectList().block()).toPrettyString());

        var result =  FluxMergeJoin.mergeJoinSortedList(
            MergeJoinType.INNER_JOIN, 
            comparator,
            predicate, 
            left.collectList(), 
            right.collectList()
        ).collectList()
        .block();

        System.out.println(MergeJoinType.INNER_JOIN + ": " + om.valueToTree(result).toPrettyString());

        result =  FluxMergeJoin.mergeJoinSortedList(
            MergeJoinType.FULL_OUTER_JOIN, 
            comparator,
            predicate, 
            left.collectList(), 
            right.collectList()
        ).collectList()
        .block();

        System.out.println(MergeJoinType.FULL_OUTER_JOIN + ": " + om.valueToTree(result).toPrettyString());
    }

    @Test
    void testHashJoin() throws JsonProcessingException {
        var om = new ObjectMapper();

        final var hashLeft = NodeHash.hashCode(
                JsonPointer.valueOf("/a/customerId")
        );

        final var hashRight = NodeHash.hashCode(
                JsonPointer.valueOf("/b/customerId")
        );

        final var sortComparator = NodeComparator.comparing(
                JsonPointer.valueOf("/a/customerId"),
                JsonPointer.valueOf("/a/customerId")
        );

        final var predicate =
                NodePredicate.alwaysTrue();
//                NodePredicate.compare(
//                        NodeComparator.comparingConst("/a/customerId", "2"), c -> c >= 0
//                );
//                .and(
//                        NodePredicate.compare(
//                                NodeComparator.comparingConst("/a/customerId", "6"), c -> c != 0
//                        )
//                );

//                        (left != null && NodeComparator.compareNodes(left.get("numberOfInquiries"), 2) >= 0) ||
//                                (right != null && NodeComparator.compareNodes(right.get("numberOfInquiries"), 6) != 0);

        var left = Mono.just(
                Arrays.asList(
                        om.readTree("{\"a\": {\"customerId\": \"1\", \"numberOfInquiries\": 1}}"),
                        om.readTree("{\"a\": {\"customerId\": \"5\", \"numberOfInquiries\": 2}}"),
                        om.readTree("{\"a\": {\"customerId\": \"2\", \"numberOfInquiries\": 4}}"),
                        om.readTree("{\"a\": {\"customerId\": \"6\", \"numberOfInquiries\": 5}}")
                )
        ).flatMapMany(list -> Flux.fromIterable(list).sort(sortComparator).cast(ObjectNode.class));

        System.out.println("LEFT: " + om.valueToTree(left.collectList().block()).toPrettyString());

        var right = Mono.just(
                Arrays.asList(
                        om.readTree("{\"b\": {\"customerId\": \"1\", \"numberOfInquiries\": 3}}"),
                        om.readTree("{\"b\": {\"customerId\": \"3\", \"numberOfInquiries\": 6}}"),
                        om.readTree("{\"b\": {\"customerId\": \"5\", \"numberOfInquiries\": 7}}")
                )
        ).flatMapMany(list -> Flux.fromIterable(list).cast(ObjectNode.class));

        System.out.println("RIGHT: " + om.valueToTree(right.collectList().block()).toPrettyString());

        var result = FluxHashMatchJoin.hashMatchJoin(HashMatchJoinType.RIGHT_OUTER_JOIN, hashLeft, hashRight, predicate, left, right)
                .collectList().onErrorStop().block();

        System.out.println("RIGHT_OUTER_JOIN: " + om.valueToTree(result).toPrettyString());

        result = FluxHashMatchJoin.hashMatchJoin(HashMatchJoinType.LEFT_OUTER_JOIN, hashLeft, hashRight, predicate, left, right)
                .collectList().block();

        System.out.println("LEFT_OUTER_JOIN: " + om.valueToTree(result).toPrettyString());

        result = FluxHashMatchJoin.hashMatchJoin(HashMatchJoinType.INNER_JOIN, hashLeft, hashRight, predicate, left, right)
                .collectList().block();

        System.out.println("INNER_JOIN: " + om.valueToTree(result).toPrettyString());
    }


}
