/*
 * Copyright (c) 2023 d-sch <https://github.com/d-sch>
 * Copyright 2023 d-sch <https://github.com/d-sch>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */
package io.github.d_sch.webfluxcommon.operators;

import java.util.ArrayList;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.github.d_sch.reactor.common.NodeComparator;
import io.github.d_sch.reactor.common.NodePredicate;
import io.github.d_sch.reactor.operators.FluxMergeJoin;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

public class FluxMergeJoinTest {

    private static ObjectMapper om = JsonMapper.builder().build();
    private Flux<ObjectNode> left;
    private Flux<ObjectNode> right;

	private NodeComparator comparator;

    @BeforeEach
    public void beforeEach() throws JsonMappingException, JsonProcessingException {

        comparator = NodeComparator.comparing(
                JsonPointer.valueOf("/a/customerId"),
                JsonPointer.valueOf("/b/customerId")
        );

        final var sortComparator = NodeComparator.comparing(
                JsonPointer.valueOf("/a/customerId"),
                JsonPointer.valueOf("/a/customerId")
        );

        left = Mono.just(
                Arrays.asList(
                        om.readTree(
                                "{\"a\": {\"customerId\": \"1\", \"numberOfInquiries\": 1}}"
                        ), om.readTree(
                                "{\"a\": {\"customerId\": \"1\", \"numberOfInquiries\": 9}}"
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
        ).sort(sortComparator);

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
                                "{\"b\": {\"customerId\": \"1\", \"numberOfInquiries\": 6}}"
                        ), om.readTree(
                                "{\"b\": {\"customerId\": \"3\", \"numberOfInquiries\": 8}}"
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

        final var predicate = NodePredicate.alwaysTrue();

        //Validate results with StepVerifier
        StepVerifier.create(
                FluxMergeJoin.innerJoin(comparator, predicate, left, right)
        ).recordWith(
                ArrayList::new
        ).expectNext(
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"5\",\"numberOfInquiries\":2},\"b\":{\"customerId\":\"5\",\"numberOfInquiries\":7}}", ObjectNode.class)                
        ).consumeRecordedWith(
                //Log results
                result -> {
                        result.forEach(item -> System.out.println("INNER_JOIN ITEM: " + om.valueToTree(item).toPrettyString()));
                }
        ).verifyComplete();
    }

    @Test
    public void testLeftOuterJoin() throws JsonMappingException, JsonProcessingException {        

        final var predicate = NodePredicate.alwaysTrue();

        StepVerifier.create(
                FluxMergeJoin.leftOuterJoin(comparator, predicate, left, right)
        ).recordWith(
                ArrayList::new
        ).expectNext(                
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"2\",\"numberOfInquiries\":4}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"5\",\"numberOfInquiries\":2},\"b\":{\"customerId\":\"5\",\"numberOfInquiries\":7}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"6\",\"numberOfInquiries\":5}}", ObjectNode.class)              
        ).consumeRecordedWith(
                //Log results
                result -> {
                        result.forEach(item -> System.out
                                .println("LEFT_OUTER_JOIN ITEM: " + om.valueToTree(item).toPrettyString()));
                }
        ).verifyComplete();
    }

    @Test
    public void testFullOuterJoin() throws JsonMappingException, JsonProcessingException {

        final var predicate = NodePredicate.alwaysTrue();

        StepVerifier.create(
                FluxMergeJoin.fullOuterJoin(
                        comparator, predicate, left, right
                )
        ).recordWith(
                ArrayList::new
        ).expectNext(                
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"2\",\"numberOfInquiries\":4}}", ObjectNode.class),
                om.readValue("{\"b\":{\"customerId\":\"3\",\"numberOfInquiries\":8}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"5\",\"numberOfInquiries\":2},\"b\":{\"customerId\":\"5\",\"numberOfInquiries\":7}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"6\",\"numberOfInquiries\":5}}", ObjectNode.class)
        ).consumeRecordedWith(
                //Log results
                result -> {
                        result.forEach(item -> System.out
                                .println("FULL_OUTER_JOIN ITEM: " + om.valueToTree(item).toPrettyString()));
                }
        ).verifyComplete();
    }

    @Test
    public void testRightOuterJoin() throws JsonMappingException, JsonProcessingException {
 
        final var predicate = NodePredicate.alwaysTrue();

        StepVerifier.create(
                FluxMergeJoin.rightOuterJoin(
                        comparator, predicate, left, right
                )
        ).recordWith(
                ArrayList::new
        ).expectNext(                
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9},\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"b\":{\"customerId\":\"3\",\"numberOfInquiries\":8}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"5\",\"numberOfInquiries\":2},\"b\":{\"customerId\":\"5\",\"numberOfInquiries\":7}}", ObjectNode.class)
        ).consumeRecordedWith(
                //Log results
                result -> {
                        result.forEach(item -> System.out
                                .println("RIGHT_OUTER_JOIN ITEM: " + om.valueToTree(item).toPrettyString()));
                }
        ).verifyComplete();
    }

    @Test
    public void testLeftSemiJoin() throws JsonMappingException, JsonProcessingException {

        final var predicate = NodePredicate.alwaysTrue();

        StepVerifier.create(
                FluxMergeJoin.leftSemiJoin(
                        comparator, predicate, left, right
                )
        ).recordWith(
                ArrayList::new
        ).expectNext(                
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":1}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"1\",\"numberOfInquiries\":9}}", ObjectNode.class),
                om.readValue("{\"a\":{\"customerId\":\"5\",\"numberOfInquiries\":2}}", ObjectNode.class)
        ).consumeRecordedWith(
                //Log results
                result -> {
                        result.forEach(item -> System.out
                                .println("LEFT_SEMI_JOIN ITEM: " + om.valueToTree(item).toPrettyString()));
                }
        ).verifyComplete();
    }

    @Test
    public void testRightSemiJoin() throws JsonMappingException, JsonProcessingException {        

        final var predicate = NodePredicate.alwaysTrue();

        StepVerifier.create(
                FluxMergeJoin.rightSemiJoin(
                        comparator, predicate, left, right
                )
        ).recordWith(
                ArrayList::new
        ).expectNext(                
                om.readValue("{\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class),
                om.readValue("{\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":3}}", ObjectNode.class),
                om.readValue("{\"b\":{\"customerId\":\"1\",\"numberOfInquiries\":6}}", ObjectNode.class)
        ).consumeRecordedWith(
                //Log results
                result -> {
                        result.forEach(item -> System.out
                                .println("RIGHT_SEMI_JOIN ITEM: " + om.valueToTree(item).toPrettyString()));
                }
        ).verifyComplete();
    }
}
