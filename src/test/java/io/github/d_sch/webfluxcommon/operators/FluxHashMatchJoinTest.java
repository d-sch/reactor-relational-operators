package io.github.d_sch.webfluxcommon.operators;

import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.d_sch.reactor.common.NodeHash;
import io.github.d_sch.reactor.common.NodePredicate;
import io.github.d_sch.reactor.operators.FluxHashMatchJoin;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class FluxHashMatchJoinTest {

    private static ObjectMapper om = new ObjectMapper();
    private Flux<ObjectNode> left;
    private Flux<ObjectNode> right;

    private NodeHash<Integer> hashLeft;
    private NodeHash<Integer> hashRight;

    @BeforeEach
    public void beforeEach() throws JsonMappingException, JsonProcessingException {

        hashLeft = NodeHash.hashCode(
                JsonPointer.valueOf("/a/customerId")
        );

        hashRight = NodeHash.hashCode(
                JsonPointer.valueOf("/b/customerId")
        );

        left = Mono.just(
                Arrays.asList(
                        om.readTree(
                                "{\"a\": {\"customerId\": \"1\", \"numberOfInquiries\": 1}}"
                        ), om.readTree(
                                "{\"a\": {\"customerId\": \"5\", \"numberOfInquiries\": 2}}"
                        ), om.readTree(
                                "{\"a\": {\"customerId\": \"2\", \"numberOfInquiries\": 4}}"
                        ), om.readTree(
                                "{\"a\": {\"customerId\": \"6\", \"numberOfInquiries\": 5}}"
                        )
                )
        ).flatMapMany(
                list -> Flux.fromIterable(
                        list
                ).cast(
                        ObjectNode.class
                )
        );

        System.out.println(
                "LEFT: " + om.valueToTree(
                        left.collectList().block()
                ).toPrettyString()
        );

        right = Mono.just(
                Arrays.asList(
                        om.readTree(
                                "{\"b\": {\"customerId\": \"1\", \"numberOfInquiries\": 3}}"
                        ), om.readTree(
                                "{\"b\": {\"customerId\": \"3\", \"numberOfInquiries\": 6}}"
                        ), om.readTree(
                                "{\"b\": {\"customerId\": \"5\", \"numberOfInquiries\": 7}}"
                        )
                )
        ).flatMapMany(
                list -> Flux.fromIterable(
                        list
                ).cast(
                        ObjectNode.class
                )
        );

        System.out.println(
                "RIGHT: " + om.valueToTree(
                        right.collectList().block()
                ).toPrettyString()
        );

    }

    @Test
    public void testInnerJoin() throws JsonMappingException, JsonProcessingException {
        var om = new ObjectMapper();

        final var predicate = NodePredicate.equals(
                JsonPointer.valueOf(
                        "/a/customerId"
                ), JsonPointer.valueOf(
                        "/b/customerId"
                )
        );

        var result = FluxHashMatchJoin.innerJoin(
                hashLeft, hashRight, predicate, left, right
        ).collectList().block();
        System.out.println(
                "INNER_JOIN: " + om.valueToTree(
                        result
                ).toPrettyString()
        );
    }

    @Test
    public void testLeftOuterJoin() throws JsonMappingException, JsonProcessingException {
        var om = new ObjectMapper();

        final var predicate = NodePredicate.equals(
                JsonPointer.valueOf(
                        "/a/customerId"
                ), JsonPointer.valueOf(
                        "/b/customerId"
                )
        );

        var result = FluxHashMatchJoin.leftOuterJoin(
                hashLeft, hashRight, predicate, left, right
        ).collectList().block();
        System.out.println(
                "INNER_JOIN: " + om.valueToTree(
                        result
                ).toPrettyString()
        );
    }

	@Test
    public void testFullOuterJoin() throws JsonMappingException, JsonProcessingException {
        var om = new ObjectMapper();

        final var predicate = NodePredicate.equals(
                JsonPointer.valueOf(
                        "/a/customerId"
                ), JsonPointer.valueOf(
                        "/b/customerId"
                )
        );

        var result = FluxHashMatchJoin.fullOuterJoin(
                hashLeft, hashRight, predicate, left, right
        ).collectList().block();
        System.out.println(
                "INNER_JOIN: " + om.valueToTree(
                        result
                ).toPrettyString()
        );
    }

	@Test
    public void testRightOuterJoin() throws JsonMappingException, JsonProcessingException {
        var om = new ObjectMapper();

        final var predicate = NodePredicate.equals(
                JsonPointer.valueOf(
                        "/a/customerId"
                ), JsonPointer.valueOf(
                        "/b/customerId"
                )
        );

        var result = FluxHashMatchJoin.rightOuterJoin(
                hashLeft, hashRight, predicate, left, right
        ).collectList().block();
        System.out.println(
                "INNER_JOIN: " + om.valueToTree(
                        result
                ).toPrettyString()
        );
    }

}
